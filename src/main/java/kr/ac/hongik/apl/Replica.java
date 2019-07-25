package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.*;
import kr.ac.hongik.apl.Operations.GreetingOperation;
import kr.ac.hongik.apl.Operations.Operation;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.diffplug.common.base.Errors.rethrow;
import static java.lang.System.exit;
import static kr.ac.hongik.apl.Messages.PrepareMessage.makePrepareMsg;
import static kr.ac.hongik.apl.Messages.PreprepareMessage.makePrePrepareMsg;
import static kr.ac.hongik.apl.Messages.ReplyMessage.makeReplyMsg;

public class Replica extends Connector {
	public static final boolean DEBUG = true;

	final static int WATERMARK_UNIT = 3;


	private final int myNumber;
	private int viewNum = 0;

	private ServerSocketChannel listener;
	private List<SocketChannel> clients;
	private PriorityQueue<CommitMessage> priorityQueue = new PriorityQueue<>(Comparator.comparingInt(CommitMessage::getSeqNum));
	//private Queue<Message> receiveBuffer = new PriorityQueue<>(Comparator.comparing(Replica::getSeqNumFromMsg));
	private PriorityBlockingQueue<Message> receiveBuffer = new PriorityBlockingQueue<>(10, Comparator.comparing(Replica::getSeqNumFromMsg));

	private static int getSeqNumFromMsg(Message message) {
		if (message instanceof HeaderMessage) {
			return -3;
		} else if (message instanceof NewViewMessage) {
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
		} else if (message instanceof ViewChangeMessage) {
			return ((ViewChangeMessage) message).getLastCheckpointNum();
		}
		throw new NoSuchElementException("getSeqNumFromMsg can't apply to " + message.getClass().toString());
	}


	private Logger logger;
	private int lowWatermark;

	private Map<String, Timer> timerMap = new ConcurrentHashMap<>();
	private AtomicBoolean isViewChangePhase = new AtomicBoolean(false);

	public Replica(Properties prop, String serverIp, int serverPort) {
		super(prop);


		String loggerFileName = String.format("consensus_%s_%d.db", serverIp, serverPort);

		this.logger = new Logger(loggerFileName);
		this.clients = new ArrayList<>();

		this.myNumber = getMyNumberFromProperty(prop, serverIp, serverPort);
		this.lowWatermark = 0;

		try {
			listener = ServerSocketChannel.open();
			listener.socket().setReuseAddress(true);
			listener.configureBlocking(false);
			listener.bind(new InetSocketAddress(serverPort));
			listener.register(this.selector, SelectionKey.OP_ACCEPT);
		} catch (IOException e) {
			System.err.println(e);
		}
		super.connect();
	}

	public static void main(String[] args) throws IOException {
		try {
			String ip = args[0];
			int port = Integer.parseInt(args[1]);
			Properties properties = new Properties();
			InputStream is = Replica.class.getResourceAsStream("/replica.properties");
			properties.load(new java.io.BufferedInputStream(is));

			Replica replica = new Replica(properties, ip, port);
			replica.start();
		} catch (ArrayIndexOutOfBoundsException e) {
			System.err.println("Usage: program <ip> <port>");
		}
	}

	/**
	 * @return
	 */
	@Override
	protected Message receive() throws InterruptedException {

		Message message;

		Class[] shouldCheckWaterMark = new Class[]{PreprepareMessage.class, PrepareMessage.class};
		Predicate<Message> isWaterMarkSensitive = (msg) -> Arrays.stream(shouldCheckWaterMark).anyMatch(msgType -> msgType.isInstance(msg));
		Class[] viewChangeUnblockTypes = new Class[]{CheckPointMessage.class, ViewChangeMessage.class, NewViewMessage.class};
		Predicate<Message> isBlockTypes = (msg) -> Arrays.stream(viewChangeUnblockTypes).noneMatch(msgType -> msgType.isInstance(msg));

		while (true) {
			message = receiveBuffer.take();

			if (getviewChangePhase()) {
				if (isBlockTypes.test(message)) {
					receiveBuffer.offer(message);
					continue;
				}
				return message;
			} else {
				if (!isWaterMarkSensitive.test(message)) {
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
		int numOfReplica = Integer.valueOf(prop.getProperty("replica"));
		for (int i = 0; i < numOfReplica; i++) {
			if (prop.getProperty("replica" + i).equals(serverIp + ":" + serverPort)) {
				return i;
			}
		}
		throw new RuntimeException("Unauthorized replica");
	}

	void start() {
		//Assume that every connection is established

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
			Message message = null;
			try {
				message = receive();
			} catch (InterruptedException e) {
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
				if (DEBUG) {
					System.err.println("got prepareMessage #" + ((PrepareMessage) message).getSeqNum() + "viewNum : " + ((PrepareMessage) message).getViewNum() + " from " + ((PrepareMessage) message).getReplicaNum());
				}
				handlePrepareMessage((PrepareMessage) message);
			} else if (message instanceof CommitMessage) {
				handleCommitMessage((CommitMessage) message);
			} else if (message instanceof CheckPointMessage) {
				if (DEBUG) {
					System.err.println("got CheckpointMessage #" + ((CheckPointMessage) message).getSeqNum() + " from " + ((CheckPointMessage) message).getReplicaNum());
				}
				handleCheckPointMessage((CheckPointMessage) message);
			} else if (message instanceof ViewChangeMessage) {
				handleViewChangeMessage((ViewChangeMessage) message);
			} else if (message instanceof NewViewMessage) {
				if (DEBUG) {
					System.err.println("got NewViewMessage from " + ((NewViewMessage) message).getNewViewNum());
				}
				handleNewViewMessage((NewViewMessage) message);
			} else
				throw new UnsupportedOperationException("Invalid message");
		}
	}

	private void loopback(Message message) {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				send(getReplicaMap().get(getMyNumber()), message);
				if (DEBUG) {
					System.err.println("Loopback: send " + message.getClass().getName());
				}
			}
		};

		new Timer().schedule(task, 10000);
	}

	/**
	 * @return
	 */
	private void handleRequestMessage(RequestMessage message) {
		ReplyMessage replyMessage = findReplyMessageOrNull(message);
		if (replyMessage != null) {
			SocketChannel destination = getChannelFromClientInfo(replyMessage.getClientInfo());
			send(destination, replyMessage);
			return;
		}

		if (this.logger.findMessage(message)) {
			return;
		}

		var sock = getChannelFromClientInfo(message.getClientInfo());
		PublicKey publicKey = publicKeyMap.get(sock);
		boolean canGoNextState = message.verify(publicKey) &&
				message.isNotRepeated(rethrow().wrap(logger::getPreparedStatement));


		if (canGoNextState) {
			logger.insertMessage(message);
			if (this.getPrimary() == this.myNumber) {
				//Enter broadcast phase
				broadcastToReplica(message);
			} else {
				//Relay to viewNum
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

	private SocketChannel getChannelFromClientInfo(PublicKey key) {
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

			PreprepareMessage preprepareMessage = makePrePrepareMsg(getPrivateKey(), getPrimary(), seqNum, message);
			logger.insertMessage(preprepareMessage);

			if (DEBUG) {
				/*
					make primary replica faulty
				 */
				int errno = 0;
				int primaryErrSeqNum = 7;
				if (seqNum == primaryErrSeqNum) {
					if (errno == 1) { //primary stops suddenly
						primaryStopCase();
					} else if (errno == 2) { //primary sends bad pre-prepare message which consists of wrong request message
						primarySendBadPrepreCase(seqNum);
					} else if (errno == 3) { // primary sends reply messages more than 2*f and does not broadcast pre-prepare message
						primarySendAllReplyMsg(seqNum, preprepareMessage);
					} else if (errno == 4) { // primary sends reply messages more than 2*f and broadcast pre-prepare message
						primarySendAllReplyMsg(seqNum, preprepareMessage);
						getReplicaMap().values().forEach(channel -> send(channel, preprepareMessage));
					} else { // normal case
						getReplicaMap().values().forEach(channel -> send(channel, preprepareMessage));
					}
					return;
				}
			}
			//Broadcast messages
			getReplicaMap().values().forEach(channel -> send(channel, preprepareMessage));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void primaryStopCase() {
		if (DEBUG)
			exit(1);
	}

	public void primarySendBadPrepreCase(int seqNum) {
		if (DEBUG) {
			InputStream in = getClass().getResourceAsStream("/replica.properties");
			Properties prop = new Properties();
			try {
				prop.load(in);
			} catch (IOException e) {
				e.printStackTrace();
			}

			Client client = new Client(prop);
			Operation op = new GreetingOperation(client.getPublicKey());
			RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
			PreprepareMessage preprepareMessage = makePrePrepareMsg(this.getPrivateKey(), this.getPrimary(), seqNum, requestMessage);
			getReplicaMap().values().forEach(channel -> send(channel, preprepareMessage));
		}
	}

	public void primarySendAllReplyMsg(int seqNum, PreprepareMessage preprepareMessage) {
		if (DEBUG) {
			PrepareMessage prepareMessage = makePrepareMsg(getPrivateKey(), getViewNum(), seqNum, preprepareMessage.getDigest(), this.myNumber);
			CommitMessage commitMessage = CommitMessage.makeCommitMsg(getPrivateKey(), getViewNum(), seqNum, prepareMessage.getDigest(), this.myNumber);
			var operation = preprepareMessage.getOperation();
			Object ret = operation.execute(this.logger);

			var viewNum = getViewNum();
			var timestamp = operation.getTimestamp();
			var clientInfo = operation.getClientInfo();
			ReplyMessage replyMessage = makeReplyMsg(getPrivateKey(), viewNum, timestamp,
					clientInfo, this.myNumber, ret, operation.isDistributed());

			logger.insertMessage(prepareMessage);
			logger.insertMessage(commitMessage);
			logger.insertMessage(seqNum, replyMessage);

			SocketChannel destination = getChannelFromClientInfo(replyMessage.getClientInfo());
			for (int i = 0; i < 4; i++)
				send(destination, replyMessage);
			return;
		}
	}
	/**
	 * @return An array which contains (low watermark, high watermark).
	 */
	public int[] getWatermarks() {
		return new int[]{this.lowWatermark, this.lowWatermark + WATERMARK_UNIT};
	}

	public int getViewNum() {
		return viewNum;
	}

	public void setViewNum(int primary) {
		this.viewNum = primary;
	}

	private int getLatestSequenceNumber() throws SQLException {
		String query = "SELECT P.seqNum FROM Preprepares P";
		PreparedStatement pstmt = logger.getPreparedStatement(query);
		ResultSet ret = pstmt.executeQuery();
		List<Integer> seqList = JdbcUtils.toStream(ret)
				.map(rethrow().wrapFunction(x -> x.getInt(1)))
				.collect(Collectors.toList());

		return seqList.isEmpty() ? getWatermarks()[0] - 1 : seqList.stream().max(Integer::compareTo).get();
	}

	private void handlePreprepareMessage(PreprepareMessage message) {
		SocketChannel primaryChannel = this.getReplicaMap().get(this.getPrimary());
		PublicKey primaryPublicKey = this.publicKeyMap.get(primaryChannel);
		PublicKey clientPublicKey = message.getRequestMessage().getClientInfo();
		SocketChannel PublicKeySocket = getChannelFromClientInfo(clientPublicKey);
		clientPublicKey = publicKeyMap.get(PublicKeySocket);


		boolean isVerified = message.isVerified(primaryPublicKey, this.getPrimary(), clientPublicKey, rethrow().wrap(logger::getPreparedStatement));

		if (isVerified) {
			logger.insertMessage(message);
			logger.insertMessage(message.getRequestMessage());
			PrepareMessage prepareMessage = makePrepareMsg(
					this.getPrivateKey(),
					message.getViewNum(),
					message.getSeqNum(),
					message.getDigest(),
					this.myNumber);
			getReplicaMap().values().forEach(channel -> send(channel, prepareMessage));
		}
	}

	private void handlePrepareMessage(PrepareMessage message) {
		PublicKey publicKey = publicKeyMap.get(this.getReplicaMap().get(message.getReplicaNum()));
		if (message.isVerified(publicKey, this.getPrimary(), this::getWatermarks)) {
			logger.insertMessage(message);
		}
		try (var pstmt = logger.getPreparedStatement("SELECT count(*) FROM Commits C WHERE C.seqNum = ? AND C.replica = ?")) {
			pstmt.setInt(1, message.getSeqNum());
			pstmt.setInt(2, this.myNumber);
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

			logger.insertMessage(commitMessage);

			getReplicaMap().values().forEach(channel -> send(channel, commitMessage));
		}
	}

	private void handleHeaderMessage(HeaderMessage message) {
		SocketChannel channel = message.getChannel();
		this.publicKeyMap.put(channel, message.getPublicKey());
		if (!publicKeyMap.containsValue(message.getPublicKey())) throw new AssertionError();

		switch (message.getType()) {
			case "replica":
				this.getReplicaMap().put(message.getReplicaNum(), channel);
				break;
			case "client":
				this.clients.add(channel);
				break;
			default:
				System.err.printf("Invalid header message: %s\n", message);
		}

	}

	private void handleCommitMessage(CommitMessage cmsg) {
		PublicKey publicKey = publicKeyMap.get(getReplicaMap().get(cmsg.getReplicaNum()));
		if (!cmsg.verify(publicKey))
			return;
		logger.insertMessage(cmsg);
		boolean isCommitted = cmsg.isCommittedLocal(rethrow().wrap(logger::getPreparedStatement),
				getMaximumFaulty(), this.myNumber);


		if (isCommitted) {
			if(priorityQueue.stream().noneMatch(x -> x.getSeqNum() == cmsg.getSeqNum()))
				priorityQueue.add(cmsg);
			try {
				while(true){
					CommitMessage rightNextCommitMsg = getRightNextCommitMsg();
					var operation = logger.getOperation(rightNextCommitMsg);

					// Release backup's view-change timer
					String key = makeKeyForTimer(rightNextCommitMsg);
					Timer timer = timerMap.remove(key);
					if (timer != null)
						timer.cancel();

					if (operation != null) {
						System.err.printf("Execute #%d\n", rightNextCommitMsg.getSeqNum());
						Object ret = operation.execute(this.logger);

						var viewNum = cmsg.getViewNum();
						var timestamp = operation.getTimestamp();
						var clientInfo = operation.getClientInfo();
						ReplyMessage replyMessage = makeReplyMsg(getPrivateKey(), viewNum, timestamp,
								clientInfo, myNumber, ret, operation.isDistributed());

						logger.insertMessage(rightNextCommitMsg.getSeqNum(), replyMessage);
						SocketChannel destination = getChannelFromClientInfo(replyMessage.getClientInfo());
						send(destination, replyMessage);
					}

					/****** Checkpoint Phase *******/
					if (rightNextCommitMsg.getSeqNum() == getWatermarks()[1] - 1) {

						int seqNum = rightNextCommitMsg.getSeqNum();
						CheckPointMessage checkpointMessage = CheckPointMessage.makeCheckPointMessage(
								this.getPrivateKey(),
								seqNum,
								logger.getStateDigest(seqNum, getMaximumFaulty()),
								this.myNumber);
						if (DEBUG) {
							System.err.println("Enter Checkpoint phase");
						}
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

	private CommitMessage getRightNextCommitMsg() throws NoSuchElementException {
		String query = "SELECT E.seqNum FROM Executed E";
		try (var pstmt = logger.getPreparedStatement(query)) {
			try (var ret = pstmt.executeQuery()) {
				List<Integer> seqList = JdbcUtils.toStream(ret)
						.map(rethrow().wrapFunction(x -> x.getInt(1)))
						.collect(Collectors.toList());

				int soFarMaxSeqNum = seqList.isEmpty() ? getWatermarks()[0] - 1 : seqList.stream().max(Integer::compareTo).get();

				var first = priorityQueue.peek();

				if (first != null && soFarMaxSeqNum + 1 == first.getSeqNum()) {
					priorityQueue.poll();
					return first;
				}
				throw new NoSuchElementException();
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw new NoSuchElementException();
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
		PublicKey publicKey = publicKeyMap.get(this.getReplicaMap().get(message.getReplicaNum()));
		if (!message.verify(publicKey))
			return;

		logger.insertMessage(message);

		if (message.getSeqNum() < getWatermarks()[0]) {
			return;
		}

		try {
			/* Check whether my checkpoint exists */
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
						if(DEBUG){
							System.err.println("start Garbage Collection");
						}
						logger.executeGarbageCollection(message.getSeqNum());
						lowWatermark += WATERMARK_UNIT;
						if(DEBUG){
							System.err.println("low watermark : "+this.lowWatermark);
						}
					}
				}

			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private void handleViewChangeMessage(ViewChangeMessage message) {
		PublicKey publicKey = publicKeyMap.get(getReplicaMap().get(message.getReplicaNum()));
		if (!message.isVerified(publicKey, this.getMaximumFaulty(), WATERMARK_UNIT))
			return;
		if (DEBUG) {
			System.err.println("got ViewChangeMessage : new view #" + message.getNewViewNum() + " from " + message.getReplicaNum());
		}
		logger.insertMessage(message);

		try {
			if (message.getNewViewNum() % getReplicaMap().size() == getMyNumber() && canMakeNewViewMessage(message)) {
				/* 정확히 2f + 1개일 때만 broadcast */
				if (DEBUG) {
					System.err.println("I'm New Primary!");
				}
				NewViewMessage newViewMessage = NewViewMessage.makeNewViewMessage(this, this.getViewNum() + 1);
				logger.insertMessage(newViewMessage);
				getReplicaMap().values().forEach(sock -> send(sock, newViewMessage));
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
					ViewChangeMessage viewChangeMessage = ViewChangeMessage.makeViewChangeMsg(
							message.getLastCheckpointNum(), minNewViewNum, this,
							getPreparedStatementFn);
					getReplicaMap().values().forEach(sock -> send(sock, viewChangeMessage));
					removeViewChangeTimer();
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
	 * @return true if DB has 2f + 1 same view number messages else false
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
		try(var psmt = logger.getPreparedStatement(query3)) {
			psmt.setInt(1, message.getNewViewNum());
			var ret = psmt.executeQuery();
			if(ret.next())
				checklist[2] = ret.getInt(1) == 0;
		}

		return Arrays.stream(checklist).allMatch(x -> x);
	}

	private void handleNewViewMessage(NewViewMessage message) {
		if (!message.isVerified(this) || message.getNewViewNum() <= this.getViewNum()) {
			if (DEBUG) {
				System.err.println("Fail newview verify");
			}
			return;
		}
		if (DEBUG) {
			System.err.println("Pass newview verify");
		}
		removeViewChangeTimer();
		removeNewViewTimer(message.getNewViewNum() + 1);

		setViewChangePhase(false);

		//Set new view number
		setViewNum(message.getNewViewNum());

		//Set new low watermark
		int newLowWatermark = message.getOperationList().get(0).getSeqNum();
		if ((getWatermarks()[0] > newLowWatermark)) throw new AssertionError();
		this.lowWatermark = newLowWatermark;

		//Execute GC
		logger.executeGarbageCollection(getWatermarks()[0] - 1);

		message.getOperationList()
				.stream()
				.map(pp -> makePrepareMsg(getPrivateKey(), pp.getViewNum(), pp.getSeqNum(), pp.getDigest(), getMyNumber()))
				.forEach(msg -> getReplicaMap().values().forEach(sock -> send(sock, msg)));
	}

	public Map<String, Timer> getTimerMap() {
		return timerMap;
	}

	public void removeNewViewTimer(int newViewNum) {
		List<String> deletableKeys = getTimerMap()
				.entrySet()
				.stream()
				.filter(x -> x.getKey().equals(generateTimerKey(newViewNum)))
				.peek(x -> x.getValue().cancel())
				.map(x -> x.getKey())
				.collect(Collectors.toList());

		getTimerMap().entrySet().removeAll(deletableKeys);
	}

	public void removeViewChangeTimer() {
		List<String> deletableKeys = getTimerMap()
				.entrySet()
				.stream()
				.filter(x -> !x.getKey().startsWith("view: "))
				.peek(x -> x.getValue().cancel())
				.map(x -> x.getKey())
				.collect(Collectors.toList());

		getTimerMap().entrySet().removeAll(deletableKeys);
	}
}
