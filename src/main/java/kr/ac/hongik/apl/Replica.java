package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.*;
import org.apache.logging.log4j.LogManager;
import org.echocat.jsu.JdbcUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.diffplug.common.base.Errors.rethrow;
import static kr.ac.hongik.apl.Messages.PrepareMessage.makePrepareMsg;
import static kr.ac.hongik.apl.Messages.PreprepareMessage.makePrePrepareMsg;
import static kr.ac.hongik.apl.Messages.ReplyMessage.makeReplyMsg;
import static kr.ac.hongik.apl.Messages.UnstableCheckPoint.makeUnstableCheckPoint;

public class Replica extends Connector {
	public static final boolean DEBUG = false;
	public final static int WATERMARK_UNIT = 10;
	public static int VERIFY_UNIT = 10; //Verify Block at every 100th insertion
	final static boolean MEASURE = false;
	private final int myNumber;
	public Object watermarkLock = new Object();
	public Object viewChangeLock = new Object();
	private int viewNum = 0;
	private ServerSocketChannel listener;
	private List<SocketChannel> clients;
	private PriorityQueue<CommitMessage> priorityQueue = new PriorityQueue<>(Comparator.comparingInt(CommitMessage::getSeqNum));
	private PriorityBlockingQueue<Message> receiveBuffer = new PriorityBlockingQueue<>(10, Comparator.comparing(Replica::getPriorityFromMsg));
	private Logger logger;
	private int lowWatermark;
	private ConcurrentHashMap<String, Timer> timerMap = new ConcurrentHashMap<>();
	private AtomicBoolean isViewChangePhase = new AtomicBoolean(false);

	private HashMap<Long, Long> receiveRequestTimeMap = new HashMap<>();
	private HashMap<Long, Long> consensusTimeMap = new HashMap<>();

	public static final org.apache.logging.log4j.Logger msgDebugger = LogManager.getLogger("msgDebugger");
	public static final org.apache.logging.log4j.Logger detailDebugger = LogManager.getLogger("detail");
	public static final org.apache.logging.log4j.Logger measureDebugger = LogManager.getLogger("measure");

	public Replica(Properties prop, String serverPublicIp, int serverPublicPort, int serverVirtualPort) {
		super(prop);
		String loggerFileName = String.format("consensus_%s_%d.db", serverPublicIp, serverPublicPort);
		this.logger = new Logger(loggerFileName);
		this.clients = new ArrayList<>();
		this.myNumber = getMyNumberFromProperty(prop, serverPublicIp, serverPublicPort);
		this.lowWatermark = 0;

		try {
			listener = ServerSocketChannel.open();
			listener.socket().setReuseAddress(true);
			listener.configureBlocking(false);
			listener.bind(new InetSocketAddress(serverVirtualPort));
			listener.register(this.selector, SelectionKey.OP_ACCEPT);
		} catch (IOException e) {
			msgDebugger.error(e);
		}
		super.connect();
	}

	private static int getSeqNumFromMsg(Message message) {
		if (message instanceof HeaderMessage) {
			return -4;
		} else if (message instanceof NewViewMessage) {
			return -3;
		} else if (message instanceof ViewChangeMessage) {
			return -2;
		} else if (message instanceof RequestMessage) {
			return -1;
		} else if (message instanceof PreprepareMessage) {
			return ((PreprepareMessage) message).getSeqNum();
		} else if (message instanceof PrepareMessage) {
			return ((PrepareMessage) message).getSeqNum();
		} else if (message instanceof CommitMessage) {
			return ((CommitMessage) message).getSeqNum();
		} else if (message instanceof CheckPointMessage) {
			return ((CheckPointMessage) message).getSeqNum();
		}
		throw new NoSuchElementException("getSeqNumFromMsg can't apply to " + message.getClass().toString());
	}

	private static int getPriorityFromMsg(Message message) {
		if (message instanceof HeaderMessage) {
			return -4;
		} else if (message instanceof NewViewMessage) {
			return -3;
		} else if (message instanceof ViewChangeMessage) {
			return -2;
		} else if (message instanceof RequestMessage) {
			return -1;
		} else if (message instanceof PreprepareMessage) {
			return ((PreprepareMessage) message).getSeqNum() + ((PreprepareMessage) message).getViewNum();
		} else if (message instanceof PrepareMessage) {
			return ((PrepareMessage) message).getSeqNum() + ((PrepareMessage) message).getViewNum();
		} else if (message instanceof CommitMessage) {
			return ((CommitMessage) message).getSeqNum() + ((CommitMessage) message).getViewNum();
		} else if (message instanceof CheckPointMessage) {
			return ((CheckPointMessage) message).getSeqNum();
		}
		throw new NoSuchElementException("getSeqNumFromMsg can't apply to " + message.getClass().toString());
	}
	public static void main(String[] args) throws IOException {
		try {
			String publicIp = args[0];
			int publicPort = Integer.parseInt(args[1]);
			int virtualPort = Integer.parseInt(args[2]);
			Properties properties = new Properties();
			InputStream is = Replica.class.getResourceAsStream("/replica.properties");
			properties.load(new java.io.BufferedInputStream(is));

			Replica replica = new Replica(properties, publicIp, publicPort, virtualPort);
			replica.start();
		} catch (ArrayIndexOutOfBoundsException e) {
			msgDebugger.error(String.format("Usage: program <ip> <port>"));
		}
	}

	void start() {
		new Thread(() -> {
			while (true) {
				try {
					receiveBuffer.put(Replica.super.receive());
				} catch (InterruptedException e) {
					continue;
				}
			}
		}).start();

		while (true) {
			Message message;
			try {
				message = receive();
			} catch (InterruptedException e) {
				msgDebugger.error("Interrupted");
				continue;
			}
			if (message instanceof HeaderMessage) {
				handleHeaderMessage((HeaderMessage) message);
			} else if (message instanceof RequestMessage) {
				if (!publicKeyMap.containsValue(((RequestMessage) message).getClientInfo()))
					loopback(message);
				else
					handleRequestMessage((RequestMessage) message);
			} else if (message instanceof PreprepareMessage) {
				if (!publicKeyMap.containsValue(((PreprepareMessage) message).getOperation().getClientInfo()))
					loopback(message);
				else
					handlePreprepareMessage((PreprepareMessage) message);
			} else if (message instanceof PrepareMessage) {
				handlePrepareMessage((PrepareMessage) message);
			} else if (message instanceof CommitMessage) {
				handleCommitMessage((CommitMessage) message);
			} else if (message instanceof CheckPointMessage) {
				handleCheckPointMessage((CheckPointMessage) message);
			} else if (message instanceof ViewChangeMessage) {
				handleViewChangeMessage((ViewChangeMessage) message);
			} else if (message instanceof NewViewMessage) {
				handleNewViewMessage((NewViewMessage) message);
			} else {
				throw new UnsupportedOperationException("Invalid message");
			}
		}
	}

	@Override
	protected Message receive() throws InterruptedException {
		Message message;
		Class[] waterMarkCheckTypes = new Class[] {PreprepareMessage.class, PrepareMessage.class};
		Class[] viewChangeUnblockTypes = new Class[] {CheckPointMessage.class, ViewChangeMessage.class, NewViewMessage.class, HeaderMessage.class };

		Predicate<Message> isWaterMarkType = (msg) -> Arrays.stream(waterMarkCheckTypes).anyMatch(msgType -> msgType.isInstance(msg));
		Predicate<Message> isBlockType = (msg) -> Arrays.stream(viewChangeUnblockTypes).noneMatch(msgType -> msgType.isInstance(msg));

		while (true) {
			message = receiveBuffer.take();
			if (getviewChangePhase()) {
				if (isBlockType.test(message)) {
					receiveBuffer.offer(message);
					continue;
				}
				return message;
			} else {
				if (!isWaterMarkType.test(message)) {
					return message;
				}
				int SeqNum = getSeqNumFromMsg(message);
				if (SeqNum < this.getWatermarks()[0]) {
					continue;
				} else if (SeqNum < this.getWatermarks()[1]) {
					return message;
				} else {
					receiveBuffer.offer(message);
				}
			}
		}
	}

	private int getMyNumberFromProperty(Properties prop, String serverIp, int serverPort) {
		int numOfReplica = Integer.parseInt(prop.getProperty("replica"));
		for (int i = 0; i < numOfReplica; i++) {
			if (prop.getProperty("replica" + i).equals(serverIp + ":" + serverPort)) {
				return i;
			}
		}
		throw new RuntimeException("Unauthorized replica");
	}

	private CommitMessage getRightNextCommitMsg() throws NoSuchElementException {
		String query = "SELECT E.seqNum FROM Executed E";
		try (var pstmt = logger.getPreparedStatement(query)) {
			try (var ret = pstmt.executeQuery()) {
				List<Integer> seqList = JdbcUtils.toStream(ret)
						.map(rethrow().wrapFunction(x -> x.getInt(1)))
						.collect(Collectors.toList());

				int soFarMaxSeqNum = seqList.isEmpty() ? getWatermarks()[0] - 1 : seqList.stream().max(Integer::compareTo).get();
				if (soFarMaxSeqNum < getWatermarks()[0])
					soFarMaxSeqNum = getWatermarks()[0] - 1;
				while(true) {
					var first = priorityQueue.peek();
					if (first != null){
						if(soFarMaxSeqNum + 1 == first.getSeqNum()){
							priorityQueue.poll();
							return first;
						} else if(first.getSeqNum() < getWatermarks()[0]){
							priorityQueue.poll();
						} else {
							throw new NoSuchElementException();
						}
					} else {
						throw new NoSuchElementException();
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw new NoSuchElementException();
		}
	}

	private void loopback(Message message) {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				send(getReplicaMap().get(getMyNumber()), message);
			}
		};
		new Timer().schedule(task, 10000);
	}

	private void handleRequestMessage(RequestMessage message) {

		msgDebugger.debug(String.format("Got Request %d", message.getTime()));

		ReplyMessage replyMessage = findReplyMessageOrNull(message);
		if (replyMessage != null) {
			SocketChannel destination = getChannelFromClientInfo(replyMessage.getClientInfo());
			send(destination, replyMessage);
			return;
		}

		detailDebugger.trace("not in reply");

		if (this.logger.findMessage(message)) {
			return;
		}

		detailDebugger.trace("not in request");

		var sock = getChannelFromClientInfo(message.getClientInfo());
		PublicKey publicKey = publicKeyMap.get(sock);
		boolean canGoNextState = message.verify(publicKey) &&
				message.isNotRepeated(rethrow().wrap(logger::getPreparedStatement));

		if (canGoNextState) {
			detailDebugger.trace("cangoNextState");
			if(MEASURE){
				receiveRequestTimeMap.put(message.getTime(), Instant.now().toEpochMilli());
			}
			logger.insertMessage(message);
			if (this.getPrimary() == this.myNumber) {
				broadcastToReplica(message);
			} else {
				super.send(getReplicaMap().get(getPrimary()), message);

				//Set a timer for view-change phase
				ViewChangeTimerTask viewChangeTimerTask = new ViewChangeTimerTask(getWatermarks()[0], this.getViewNum() + 1, this);
				Timer timer = new Timer();
				timer.schedule(viewChangeTimerTask, Replica.TIMEOUT);

				/* Store timer object to cancel it when the request is executed and the timer is not expired.
				 * key: operation, value: timer
				 * An operation can be a key because every operation has a random UUID;
				 */
				String key = Util.hash(message.getOperation());
				timerMap.put(key, timer);
			}
		}
	}

	private ReplyMessage findReplyMessageOrNull(RequestMessage requestMessage) {
		try (var pstmt = logger.getPreparedStatement("SELECT PP.seqNum FROM PrePrepares PP WHERE PP.requestMessage = ?")) {
			pstmt.setString(1, Util.serToString(requestMessage));
			var ret = pstmt.executeQuery();
			if (ret.next()) {
				int seqNum = ret.getInt(1);

				try (var pstmt1 = logger.getPreparedStatement("SELECT E.replyMessage FROM Executed E WHERE E.seqNum = ?")) {
					pstmt1.setInt(1, seqNum);
					var ret1 = pstmt1.executeQuery();
					if (ret1.next())
						return Util.desToObject(ret1.getString(1), ReplyMessage.class);
					else
						return null;
				}
			} else {
				return null;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	public SocketChannel getChannelFromClientInfo(PublicKey key) {
		return publicKeyMap.entrySet().stream()
				.filter(x -> x.getValue().equals(key))
				.findFirst().get().getKey();
	}

	public int getPrimary() {
		return viewNum % getReplicaMap().size();
	}

	private void broadcastToReplica(RequestMessage message) {
		try {
			int seqNum = getLatestSequenceNumber() + 1;
			PreprepareMessage preprepareMessage = makePrePrepareMsg(getPrivateKey(), getViewNum(), seqNum, message);
			logger.insertMessage(preprepareMessage);
			getReplicaMap().values().forEach(channel -> send(channel, preprepareMessage));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return An array which contains (low watermark, high watermark).
	 */
	public int[] getWatermarks() {
		return new int[] {this.lowWatermark, this.lowWatermark + WATERMARK_UNIT};
	}

	public int getViewNum() {
		return viewNum;
	}

	public void setViewNum(int primary) {
		this.viewNum = primary;
	}

	protected int getLatestSequenceNumber() throws SQLException {
		String query = "SELECT P.seqNum FROM Preprepares P where viewNum = ?";
		PreparedStatement pstmt = logger.getPreparedStatement(query);
		pstmt.setInt(1, this.getViewNum());
		ResultSet ret = pstmt.executeQuery();
		List<Integer> seqList = JdbcUtils.toStream(ret)
				.map(rethrow().wrapFunction(x -> x.getInt(1)))
				.collect(Collectors.toList());
		return seqList.isEmpty() ? getWatermarks()[0] - 1 : seqList.stream().max(Integer::compareTo).get();
	}

	private void handlePreprepareMessage(PreprepareMessage message) {

		msgDebugger.debug(String.format("Got Pre-pre msg view : %d seq : %d request timestamp : %d", message.getViewNum(), message.getSeqNum(), message.getRequestMessage().getTime()));

		SocketChannel primaryChannel = this.getReplicaMap().get(this.getPrimary());
		PublicKey primaryPublicKey = this.publicKeyMap.get(primaryChannel);
		PublicKey clientPublicKey = message.getRequestMessage().getClientInfo();
		SocketChannel PublicKeySocket = getChannelFromClientInfo(clientPublicKey);
		clientPublicKey = publicKeyMap.get(PublicKeySocket);
		boolean isVerified = message.isVerified(primaryPublicKey, this.getViewNum(), clientPublicKey, rethrow().wrap(logger::getPreparedStatement));

		if (isVerified) {
			logger.insertMessage(message);
			logger.insertMessage(message.getRequestMessage());
			PrepareMessage prepareMessage = makePrepareMsg(
					this.getPrivateKey(),
					message.getViewNum(),
					message.getSeqNum(),
					message.getDigest(),
					this.myNumber);
			logger.insertMessage(prepareMessage);
			getReplicaMap().values().forEach(channel -> send(channel, prepareMessage));
		}
	}

	private void handlePrepareMessage(PrepareMessage message) {

		msgDebugger.debug(String.format("Got Prepare msg view : %d seq : %d from %d", message.getViewNum(), message.getSeqNum(), message.getReplicaNum()));

		PublicKey publicKey = publicKeyMap.get(getReplicaMap().get(message.getReplicaNum()));
		if (message.isVerified(publicKey, this.getViewNum(), this::getWatermarks)) {
			logger.insertMessage(message);
		}
		try (var pstmt = logger.getPreparedStatement("SELECT count(*) FROM Commits C WHERE C.seqNum = ? AND C.replica = ? AND C.digest = ?")) {
			pstmt.setInt(1, message.getSeqNum());
			pstmt.setInt(2, this.myNumber);
			pstmt.setString(3, message.getDigest());
			try (var ret = pstmt.executeQuery()) {
				if (ret.next()) {
					var i = ret.getInt(1);
					if (i > 0)
						return;
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		}

		if (message.isPrepared(rethrow().wrap(logger::getPreparedStatement), getMaximumFaulty(), this.myNumber)) {
			CommitMessage commitMessage = CommitMessage.makeCommitMsg(
					this.getPrivateKey(),
					message.getViewNum(),
					message.getSeqNum(),
					message.getDigest(),
					this.myNumber);

			handleCommitMessage(commitMessage);

			getReplicaMap().values().forEach(channel -> send(channel, commitMessage));
		}
	}

	private void handleHeaderMessage(HeaderMessage message) {
		SocketChannel channel = message.getChannel();
		this.publicKeyMap.put(channel, message.getPublicKey());
		if (!publicKeyMap.containsValue(message.getPublicKey())) throw new AssertionError();

		switch (message.getType()) {
			case "replica":
				getReplicaMap().put(message.getReplicaNum(), channel);
				break;
			case "client":
				this.clients.add(channel);
				break;
			default:
				msgDebugger.error(String.format("Invalid header message : %s", message));
		}
	}

	private void handleCommitMessage(CommitMessage cmsg) {

		msgDebugger.debug(String.format("Got Commit msg view : %d seq : %d from %d", cmsg.getViewNum(), cmsg.getSeqNum(), cmsg.getReplicaNum()));

		PublicKey publicKey = publicKeyMap.get(getReplicaMap().get(cmsg.getReplicaNum()));
		if (!cmsg.verify(publicKey))
			return;
		logger.insertMessage(cmsg);
		boolean isCommitted = cmsg.isCommittedLocal(rethrow().wrap(logger::getPreparedStatement),
				getMaximumFaulty(), this.myNumber);

		if (isCommitted) {
			if(MEASURE){
				Long key = logger.getOperation(cmsg).getTimestamp();
				consensusTimeMap.put(key, Instant.now().toEpochMilli() - receiveRequestTimeMap.get(key));
				double duration = (double)(consensusTimeMap.get(key)) / 1000;

				measureDebugger.info(String.format("Consensus Completed #%d\t%f sec\n", cmsg.getSeqNum(), duration));
			}
			if (priorityQueue.stream().noneMatch(x -> x.getSeqNum() == cmsg.getSeqNum()))
				priorityQueue.add(cmsg);
			try {
				while (true) {
					CommitMessage rightNextCommitMsg = getRightNextCommitMsg();
					var operation = logger.getOperation(rightNextCommitMsg);
					// Release backup's view-change timer
					String key = makeKeyForTimer(rightNextCommitMsg);
					Timer timer = timerMap.remove(key);
					Optional.ofNullable(timer).ifPresent(Timer::cancel);

					if (operation != null) {
						msgDebugger.info(String.format("Executed #%d", rightNextCommitMsg.getSeqNum()));
						Object ret = operation.execute(this.logger);
						if(MEASURE) {
							if (rightNextCommitMsg.getSeqNum()%10 == 0) {
								printConsensusTime();
							}
						}
						var viewNum = cmsg.getViewNum();
						var timestamp = operation.getTimestamp();
						var clientInfo = operation.getClientInfo();
						ReplyMessage replyMessage = makeReplyMsg(getPrivateKey(), viewNum, timestamp,
								clientInfo, myNumber, ret);

						logger.insertMessage(rightNextCommitMsg.getSeqNum(), replyMessage);
						SocketChannel destination = getChannelFromClientInfo(replyMessage.getClientInfo());
						send(destination, replyMessage);

						UnstableCheckPoint unstableCheckPoint = makeUnstableCheckPoint(getPrivateKey(), this.getWatermarks()[0], rightNextCommitMsg.getSeqNum(), rightNextCommitMsg.getDigest());
						logger.insertMessage(unstableCheckPoint);
					}

					/****** Checkpoint Phase *******/
					if (rightNextCommitMsg.getSeqNum() == getWatermarks()[1] - 1) {

						int seqNum = rightNextCommitMsg.getSeqNum();
						CheckPointMessage checkpointMessage = CheckPointMessage.makeCheckPointMessage(
								this.getPrivateKey(),
								seqNum,
								logger.getStateDigest(seqNum, getMaximumFaulty(), rightNextCommitMsg.getViewNum()),
								this.myNumber);
						//Broadcast message
						getReplicaMap().values().forEach(sock -> send(sock, checkpointMessage));
					}
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (NoSuchElementException e) {
				return;
			}
		}
	}

	/**
	 * Generate key for getting timer from timerMap.
	 * Commit message doesn't have an operation, so the replica need to query from logger.
	 *
	 * @param cmsg
	 * @return Hash(operation) which is queried from cmsg's request
	 */
	private String makeKeyForTimer(CommitMessage cmsg) {
		var operation = logger.getOperation(cmsg);
		return Util.hash(operation);
	}

	private void handleCheckPointMessage(CheckPointMessage message) {

		msgDebugger.debug(String.format("Got Checkpoint msg seq : %d from %d", message.getSeqNum(), message.getReplicaNum()));

		PublicKey publicKey = publicKeyMap.get(this.getReplicaMap().get(message.getReplicaNum()));
		if (!message.verify(publicKey))
			return;

		logger.insertMessage(message);

		if (message.getSeqNum() < getWatermarks()[0]) {
			return;
		}
		try {
			String query = "SELECT count(*) FROM Checkpoints C WHERE C.seqNum = ? AND C.replica = ?";
			try (var psmt = logger.getPreparedStatement(query)) {
				psmt.setInt(1, message.getSeqNum());
				psmt.setInt(2, this.myNumber);
				var ret = psmt.executeQuery();
				ret.next();
				if (ret.getInt(1) != 1)
					return;
			}
			query = "SELECT stateDigest FROM Checkpoints C WHERE C.seqNum = ?";
			try (var psmt = logger.getPreparedStatement(query)) {
				psmt.setInt(1, message.getSeqNum());
				try (var ret = psmt.executeQuery()) {
					Map<String, Integer> digestMap = new HashMap<>();
					while (ret.next()) {
						var key = ret.getString(1);
						var num = digestMap.getOrDefault(key, 0);
						digestMap.put(key, num + 1);
					}
					int max = digestMap
							.values()
							.stream()
							.max(Comparator.comparingInt(x -> x))
							.orElse(0);

					if (max > 2 * getMaximumFaulty() && message.getSeqNum() > this.lowWatermark) {
						synchronized (watermarkLock) {
							logger.executeGarbageCollection(message.getSeqNum());
							lowWatermark += WATERMARK_UNIT;

							detailDebugger.trace("GARBAGE COLLECTION DONE -> new low watermark : %d", lowWatermark);

						}
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void handleViewChangeMessage(ViewChangeMessage message) {

		msgDebugger.debug(String.format("Got ViewChange msg NEWVIEW : %d from %d", message.getNewViewNum(), message.getReplicaNum()));

		PublicKey publicKey = publicKeyMap.get(getReplicaMap().get(message.getReplicaNum()));
		if (!message.isVerified(publicKey, this.getMaximumFaulty(), WATERMARK_UNIT)) {
			return;
		}
		logger.insertMessage(message);
		if (this.getViewNum() >= message.getNewViewNum()) {
			return;
		}
		try {
			if (message.getNewViewNum() % getReplicaMap().size() == getMyNumber() && canMakeNewViewMessage(message)) {
				/* 정확히 2f + 1개일 때만 broadcast */

				msgDebugger.debug(String.format("Send NewViewMessage"));
				NewViewMessage newViewMessage = NewViewMessage.makeNewViewMessage(this, message.getNewViewNum());
				logger.insertMessage(newViewMessage);
				getReplicaMap().values().forEach(sock -> send(sock, newViewMessage));
				handleNewViewMessage(newViewMessage);
			} else if (hasTwoFPlusOneMessages(message)) {
				/* 2f + 1개 이상의 v+i에 해당하는 메시지 수집 -> new view를 기다리는 동안 timer 작동 */
				ViewChangeTimerTask task = new ViewChangeTimerTask(getWatermarks()[0], message.getNewViewNum() + 1, this);
				Timer timer = new Timer();
				timer.schedule(task, TIMEOUT * (message.getNewViewNum() + 1 - this.getViewNum()));

				timerMap.put(generateTimerKey(message.getNewViewNum() + 1), timer);
			} else {
				/* f + 1 이상의 v > currentView 인 view-change를 수집한다면
					나 자신도 f + 1개의 view-change 중 min-view number로 view-change message를 만들어 배포한다. */
				String isAlreadyInsertedQuery = "SELECT count(*) FROM Viewchanges V WHERE V.newViewNum = ? AND V.replica = ?";
				try (var pstmt = logger.getPreparedStatement(isAlreadyInsertedQuery)) {
					pstmt.setInt(1, message.getNewViewNum());
					pstmt.setInt(2, getMyNumber());
					ResultSet rs = pstmt.executeQuery();
					rs.next();
					if (rs.getInt(1) != 0) {
						return;
					}
				}
				List<Integer> newViewNumList;
				String query = "SELECT V1.newViewNum FROM Viewchanges V1 "
						+ "WHERE V1.newViewNum > ? AND "
						+ "NOT V1.newViewNum IN "
						+ "( SELECT V2.newViewNum FROM Viewchanges V2 WHERE V2.replica = ? )";
				try (var pstmt = logger.getPreparedStatement(query)) {
					pstmt.setInt(1, this.getViewNum());
					pstmt.setInt(2, this.getMyNumber());
					ResultSet rs = pstmt.executeQuery();
					newViewNumList = JdbcUtils.toStream(rs)
							.map(rethrow().wrapFunction(x -> x.getString(1)))
							.map(Integer::valueOf)
							.sorted()
							.collect(Collectors.toList());
				}
				int minNewViewNum = newViewNumList.stream().min(Integer::min).get();

				if (newViewNumList.size() == getMaximumFaulty() + 1) {
					this.setViewChangePhase(true);
					var getPreparedStatementFn = rethrow().wrap(getLogger()::getPreparedStatement);

					message.getCheckPointMessages().stream().forEach(x -> logger.insertMessage(x));
					synchronized (watermarkLock) {
						removeViewChangeTimer();
						ViewChangeMessage viewChangeMessage = ViewChangeMessage.makeViewChangeMsg(
								message.getLastCheckpointNum(), minNewViewNum, this,
								getPreparedStatementFn);
						getReplicaMap().values().forEach(sock -> send(sock, viewChangeMessage));
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public String generateTimerKey(int newViewNum) {
		return "view: " + (newViewNum);
	}

	public int getMyNumber() {
		return this.myNumber;
	}

	@Override
	protected void sendHeaderMessage(SocketChannel channel) {
		HeaderMessage headerMessage = new HeaderMessage(this.myNumber, this.getPublicKey(), "replica");
		send(channel, headerMessage);
	}

	@Override
	protected void acceptOp(SelectionKey key) {
		SocketChannel channel = (SocketChannel) key.channel();
		clients.add(channel);
	}

	/**
	 * @param message View-change message
	 * @return true if DB has f + 1 same view number messages else false
	 */
	private boolean hasTwoFPlusOneMessages(ViewChangeMessage message) {
		String query = "SELECT newViewNum FROM Viewchanges V WHERE V.newViewNum = ? ";
		try (var pstmt = logger.getPreparedStatement(query)) {
			pstmt.setInt(1, message.getNewViewNum());
			var ret = pstmt.executeQuery();

			return JdbcUtils.toStream(ret)
					.map(rethrow().wrapFunction(row -> row.getString(1)))
					.map(Integer::valueOf)
					.count() == 2 * getMaximumFaulty() + 1;

		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public Logger getLogger() {
		return this.logger;
	}

	public boolean getviewChangePhase() {
		return this.isViewChangePhase.get();
	}

	public void setViewChangePhase(boolean viewChangePhase) {
		isViewChangePhase.set(viewChangePhase);
	}

	private boolean canMakeNewViewMessage(ViewChangeMessage message) throws SQLException {
		Boolean[] checklist = new Boolean[3];
		String query1 = "SELECT count(*) FROM ViewChanges V WHERE V.replica = ? AND V.newViewNum = ?";
		try (var pstmt = logger.getPreparedStatement(query1)) {
			pstmt.setInt(1, this.myNumber);
			pstmt.setInt(2, message.getNewViewNum());
			var ret = pstmt.executeQuery();
			if (ret.next())
				checklist[0] = ret.getInt(1) == 1;
		}
		String query2 = "SELECT count(*) FROM ViewChanges V WHERE V.replica <> ? AND V.newViewNum = ?";
		try (var pstmt = logger.getPreparedStatement(query2)) {
			pstmt.setInt(1, this.myNumber);
			pstmt.setInt(2, message.getNewViewNum());
			var ret = pstmt.executeQuery();
			if (ret.next())
				checklist[1] = ret.getInt(1) >= 2 * getMaximumFaulty();
		}
		String query3 = "SELECT count(*) FROM NewViewMessages WHERE newViewNum = ?";
		try (var psmt = logger.getPreparedStatement(query3)) {
			psmt.setInt(1, message.getNewViewNum());
			var ret = psmt.executeQuery();
			if (ret.next())
				checklist[2] = ret.getInt(1) == 0;
		}
		return Arrays.stream(checklist).allMatch(x -> x);
	}

	private void handleNewViewMessage(NewViewMessage message) {

		msgDebugger.debug(String.format("Got Newview msg from %d", message.getNewViewNum()));
		if(message.getNewViewNum()%getReplicaMap().size() == getMyNumber()) {
			msgDebugger.info(String.format("I'm New Primary"));
		}

		if (!message.isVerified(this) || message.getNewViewNum() <= this.getViewNum()) {
			return;
		}
		this.logger.insertMessage(message);

		removeViewChangeTimer();
		removeNewViewTimer(message.getNewViewNum() + 1);
		synchronized (viewChangeLock) {
			setViewChangePhase(false);
			setViewNum(message.getNewViewNum());
		}
		int newLowWatermark = message.getViewChangeMessageList()
				.stream()
				.max(Comparator.comparingInt(ViewChangeMessage::getLastCheckpointNum))
				.get()
				.getLastCheckpointNum();

		synchronized (watermarkLock) {
			this.lowWatermark = getWatermarks()[0] > newLowWatermark ? getWatermarks()[0] : newLowWatermark;
			message.getViewChangeMessageList()
					.stream()
					.flatMap(x -> x.getCheckPointMessages().stream())
					.forEach(c -> logger.insertMessage(c));
			logger.executeGarbageCollection(getWatermarks()[0] - 1);
		}

		if (message.getOperationList() != null) {
			message.getOperationList()
					.stream()
					.forEach(pp -> handlePreprepareMessage(pp));
		}
	}

	public Map<String, Timer> getTimerMap() {
		return timerMap;
	}

	public void removeNewViewTimer(int newViewNum) {
		List<String> deletableKeys = getTimerMap().keySet().stream().filter(x -> x.equals(generateTimerKey(newViewNum))).collect(Collectors.toList());
		deletableKeys.stream().forEach(x -> getTimerMap().get(x).cancel());
		deletableKeys.stream().forEach(x -> getTimerMap().remove(x));
	}

	public void removeViewChangeTimer() {
		//TODO : equals("view")를 교체해야 함
		List<String> deletableKeys = getTimerMap().keySet().stream().filter(x -> !x.equals("view")).collect(Collectors.toList());
		deletableKeys.stream().forEach(x -> getTimerMap().get(x).cancel());
		deletableKeys.stream().forEach(x -> getTimerMap().remove(x));
	}
	public void printConsensusTime(){
		if(MEASURE) {
			double avg = consensusTimeMap
					.values()
					.stream()
					.mapToLong(Long::longValue)
					.average()
					.orElse(0);
			measureDebugger.info(String.format("Average Consensus Time : ", avg/1000));
		}
	}
}