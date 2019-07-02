package kr.ac.hongik.apl;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.diffplug.common.base.Errors.rethrow;
import static kr.ac.hongik.apl.PrepareMessage.makePrepareMsg;
import static java.lang.Thread.sleep;
import static kr.ac.hongik.apl.PreprepareMessage.makePrePrepareMsg;
import static kr.ac.hongik.apl.ReplyMessage.makeReplyMsg;
/*TODO
	connector의 receive 함수 내부에서 deserialize시 터지는 문제가 간헐적으로 발생함.
	kafka로 전환하며 해결할 예정.
 */

public class Replica extends Connector {
	public static final boolean DEBUG = true;

	final static int WATERMARK_UNIT = 100;


	private final int myNumber;
	private int viewNum = 0;

	private ServerSocketChannel listener;
	private List<SocketChannel> clients;
	private PriorityQueue<CommitMessage> priorityQueue = new PriorityQueue<>(Comparator.comparingInt(CommitMessage::getSeqNum));

	private Logger logger;
	private int lowWatermark;

	private Map<String, Timer> timerMap = new HashMap<>();
	private AtomicBoolean isViewChangePhase = new AtomicBoolean(false);
	private Queue<Message> buffer = new LinkedList<>();


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
	 * @return An array which contains (low watermark, high watermark).
	 */
	int[] getWatermarks() {
		return new int[]{this.lowWatermark, this.lowWatermark + WATERMARK_UNIT};
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
		while (true) {
			Message message = receive();              //Blocking method
			if (message instanceof HeaderMessage) {
				handleHeaderMessage((HeaderMessage) message);
			} else if (message instanceof RequestMessage) {
				handleRequestMessage((RequestMessage) message);
			} else if (message instanceof PreprepareMessage) {
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
			} else
				throw new UnsupportedOperationException("Invalid message");
		}
	}

	/**
	 * @return
	 */
	@Override
	protected Message receive() {
		if (!getviewChangePhase()) {
			if (buffer.isEmpty())
				return super.receive();
			else
				return buffer.poll();
		} else {
			Class[] messageTypes = new Class[]{CheckPointMessage.class, ViewChangeMessage.class, NewViewMessage.class};
			Predicate<Message> receivable = (msg) -> Arrays.stream(messageTypes).anyMatch(msgType -> msgType.isInstance(msg));

			Message message;
			for (message = super.receive(); !receivable.test(message); message = super.receive()) {
				buffer.offer(message);
			}
			return message;
		}
	}

	private void handleRequestMessage(RequestMessage message) {
		ReplyMessage replyMessage = findReplyMessageOrNull(message.getOperation());
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
				super.send(replicas.get(getPrimary()), message);

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

	private ReplyMessage findReplyMessageOrNull(Operation operation) {
		try (var pstmt = logger.getPreparedStatement("SELECT PP.seqNum FROM PrePrepares PP WHERE PP.operation = ?")) {
			pstmt.setString(1, Util.serToString(operation));
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

	private void handlePreprepareMessage(PreprepareMessage message) {
		SocketChannel primaryChannel = this.replicas.get(this.getPrimary());
		PublicKey publicKey = this.publicKeyMap.get(primaryChannel);
		boolean isVerified = message.isVerified(publicKey, this.getPrimary(), this::getWatermarks, rethrow().wrap(logger::getPreparedStatement));

		if (isVerified) {
			logger.insertMessage(message);
			PrepareMessage prepareMessage = makePrepareMsg(
					this.getPrivateKey(),
					message.getViewNum(),
					message.getSeqNum(),
					message.getDigest(),
					this.myNumber);
			replicas.values().forEach(channel -> send(channel, prepareMessage));
		} else {
			/*
			TODO
			Watermark update가 이루어지기 전에 H 보다 큰 request가 올 경우 리젝되는 문제가 발생하여 임시로 해당 메시지를 TCP 윈도우에 넣도록 하였다.
			추후에 viewchange와 통합시 viewchange에 있는 queue를 사용하도록 수정할 예정이다.
			 */
            if (DEBUG) {
                System.err.println(this.lowWatermark + " " + message.getSeqNum());
            }
            send(replicas.get(this.myNumber), message);
        }

	}

	private void handlePrepareMessage(PrepareMessage message) {
		PublicKey publicKey = publicKeyMap.get(this.replicas.get(message.getReplicaNum()));
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

			replicas.values().forEach(channel -> send(channel, commitMessage));
		}
	}

	private void handleHeaderMessage(HeaderMessage message) {
		SocketChannel channel = message.getChannel();
		this.publicKeyMap.put(channel, message.getPublicKey());
		if (!publicKeyMap.containsValue(message.getPublicKey())) throw new AssertionError();

		switch (message.getType()) {
			case "replica":
				this.replicas.put(message.getReplicaNum(), channel);
				break;
			case "client":
				this.clients.add(channel);
				break;
			default:
				System.err.printf("Invalid header message: %s\n", message);
		}

	}

	private CommitMessage getRightNextCommitMsg() throws NoSuchElementException {
		String query = "SELECT MAX(E.seqNum) FROM Executed E";
		try (var pstmt = logger.getPreparedStatement(query)) {
			try (var ret = pstmt.executeQuery()) {
				ret.next();
				int soFarMaxSeqNum = ret.getInt(1);
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


	boolean isAlreadyExecuted(int sequenceNumber) {
		String query = "SELECT count(*) FROM Executed E WHERE E.seqNum = ?";
		try (var pstmt = logger.getPreparedStatement(query)) {
			pstmt.setInt(1, sequenceNumber);
			try (var ret = pstmt.executeQuery()) {
				if (ret.next())
					return ret.getInt(1) > 0;
				else
					return false;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}

	}

	private void handleCommitMessage(CommitMessage cmsg) {
		boolean isCommitted = cmsg.isCommittedLocal(rethrow().wrap(logger::getPreparedStatement),
				getMaximumFaulty(), this.myNumber);

		logger.insertMessage(cmsg);

		if (isCommitted) {

			/**
			 * 이미 합의가 이루어져서 실행이 된 경우 executed table에 삽입이 될 것이므로,
			 * 이를 이용하여 합의 여부를 감지한다.
			 *
			 * 이미 수행된 경우에는 수행된 값을 DB로부터 읽어 반환한다.
			 */
			if (isAlreadyExecuted(cmsg.getSeqNum())) {
				try (var pstmt = logger.getPreparedStatement("SELECT replyMessage FROM Executed WHERE seqNum = ?")) {
					pstmt.setInt(1, cmsg.getSeqNum());
					var ret = pstmt.executeQuery();
					ReplyMessage replyMessage = JdbcUtils.toStream(ret)
							.map(rethrow().wrapFunction(x -> x.getString(1)))
							.map(x -> Util.desToObject(x, ReplyMessage.class))
							.findFirst()
							.get();

					SocketChannel destination = getChannelFromClientInfo(replyMessage.getClientInfo());
					send(destination, replyMessage);
					return;

				} catch (SQLException e) {
					e.printStackTrace();
					return;
				}
			}


			priorityQueue.add(cmsg);
			try {
				CommitMessage rightNextCommitMsg = getRightNextCommitMsg();
				var operation = logger.getOperation(rightNextCommitMsg);

				if (operation == null)
					return;

				if (operation instanceof BlockCreation) {
					((BlockCreation) operation).setSqlAccessor(rethrow().wrap(logger::getPreparedStatement));
				} else if (operation instanceof CertStorage) {
					((CertStorage) operation).setSqlAccessor(rethrow().wrap(logger::getPreparedStatement));
					((CertStorage) operation).setReplicaNumber(myNumber);
				} else if (operation instanceof Collector) {
					((Collector) operation).setReplicaNumber(myNumber);
					((Collector) operation).setSqlAccessor(rethrow().wrap(logger::getPreparedStatement));
				} else if (operation instanceof Validation) {
					((Validation) operation).setSqlAccessor(rethrow().wrap(logger::getPreparedStatement));
				}

				// Release backup's view-change timer
				String key = makeKeyForTimer(rightNextCommitMsg);
				Timer timer = timerMap.remove(key);
				timer.cancel();


				Object ret = operation.execute();

				System.err.printf("Execute #%d\n", cmsg.getSeqNum());

				var viewNum = cmsg.getViewNum();
				var timestamp = operation.getTimestamp();
				var clientInfo = operation.getClientInfo();
				ReplyMessage replyMessage = makeReplyMsg(getPrivateKey(), viewNum, timestamp,
						clientInfo, myNumber, ret, operation.isDistributed());

				logger.insertMessage(rightNextCommitMsg.getSeqNum(), replyMessage);
				SocketChannel destination = getChannelFromClientInfo(replyMessage.getClientInfo());
				send(destination, replyMessage);

				if(rightNextCommitMsg.getSeqNum() == getWatermarks()[1]) {
					int seqNum = rightNextCommitMsg.getSeqNum();
                    CheckPointMessage checkpointMessage = CheckPointMessage.makeCheckPointMessage(
                            this.getPrivateKey(),
                            seqNum,
							logger.getStateDigest(seqNum),
							this.myNumber);
					//Broadcast message
					replicas.values().forEach(sock -> send(sock, checkpointMessage));
				}

			} catch (SQLException e) {
				e.printStackTrace();
			}
			catch (NoSuchElementException e) {
				return;
			}
		}
	}

	/**
	 * Generate key for getting timer from timerMap.
	 * Commit message doesn't have an operation, so the replica need to query from logger.
	 *
	 *
	 * @param cmsg
	 * @return Hash(operation) which is queried from cmsg's request
	 */
	private String makeKeyForTimer(CommitMessage cmsg) {
		var operation = logger.getOperation(cmsg);
		return Util.hash(operation);
	}

	private void handleCheckPointMessage(CheckPointMessage message){
		PublicKey publicKey = publicKeyMap.get(this.replicas.get(message.getReplicaNum()));
		if(message.verify(publicKey)) {
			logger.insertMessage(message);

			String query = "SELECT stateDigest FROM Checkpoints C WHERE C.seqNum = ?";

			try(var psmt = logger.getPreparedStatement(query)) {

				psmt.setInt(1,message.getSeqNum());

				try(var ret = psmt.executeQuery()) {
					Map<String, Integer> digestMap = new HashMap<>();
					while(ret.next()) {
						var key = ret.getString(1);
						var num = digestMap.getOrDefault(key, 0);
						digestMap.put(key, num + 1);
					}
					int f = getMaximumFaulty();
					int max = digestMap
							.values()
							.stream()
							.max(Comparator.comparingInt(x -> x))
							.orElse(0);

					if (max == 2 * f + 1) {
						if (DEBUG) {
							System.err.println("start in GC");
						}
						logger.executeGarbageCollection(message.getSeqNum());
                        lowWatermark += WATERMARK_UNIT;
					}
				}

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}



	private SocketChannel getChannelFromClientInfo(PublicKey key) {
		return publicKeyMap.entrySet().stream()
				.filter(x -> x.getValue().equals(key))
				.findFirst().get().getKey();
	}

	public int getPrimary() {
		return viewNum % replicas.size();
	}

	public int getViewNum() {
		return viewNum;
	}

	public void setViewNum(int primary) {
		this.viewNum = primary;
	}

	private void handleViewChangeMessage(ViewChangeMessage message) {
		PublicKey publicKey = publicKeyMap.get(replicas.get(message.getReplicaNum()));
		//TODO: ViewChange msg안의 C 집합 select n from checkpoints group by digest,n having count(*) > 2*f
		if (!message.verify(publicKey))
			return;

		logger.insertMessage(message);


		try {
			if (message.getNewViewNum() % replicas.size() == getMyNumber() && canMakeNewViewMessage(message)) {
				/* 정확히 2f + 1개일 때만 broadcast */
				NewViewMessage newViewMessage = NewViewMessage.makeNewViewMessage(this, this.getViewNum() + 1);
				replicas.values().forEach(sock -> send(sock, newViewMessage));
			} else if (hasTwoFPlusOneMessages(message)) {
				/* 2f + 1개 이상의 v+i에 해당하는 메시지 수집 -> new view를 기다리는 동안 timer 작동 */
				ViewChangeTimerTask task = new ViewChangeTimerTask(getWatermarks()[0], message.getNewViewNum() + 1, this);
				Timer timer = new Timer();
				timer.schedule(task, TIMEOUT * (message.getNewViewNum() + 1 - this.getViewNum()));

				String key = "view: " + (message.getNewViewNum() + 1);
				timerMap.put(key, timer);
			} else {
				/* f + 1 이상의 v > currentView 인 view-change를 수집한다면
					나 자신도 f + 1개의 view-change 중 min-view number로 view-change message를 만들어 배포한다. */

				List<Integer> newViewList;
				String query = "SELECT V.newViewNum from Viewchanges V WHERE V.newViewNum > ?";
				try (var pstmt = logger.getPreparedStatement(query)) {
					pstmt.setInt(1, this.getMyNumber());
					ResultSet rs = pstmt.executeQuery();
					newViewList = JdbcUtils.toStream(rs)
							.map(rethrow().wrapFunction(x -> x.getString(1)))
							.map(Integer::valueOf)
							.sorted()
							.collect(Collectors.toList());
				}

				if (newViewList.size() == getMaximumFaulty() + 1) {
					NewViewMessage newViewMessage = NewViewMessage.makeNewViewMessage(this, newViewList.get(0));
					replicas.values().forEach(sock -> send(sock, newViewMessage));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param message View-change message
	 * @return true if DB has f + 1 same view number messages else false
	 */
	private boolean hasTwoFPlusOneMessages(ViewChangeMessage message) {
		String query = "SELECT V.newViewNum FROM Viewchanges V WHERE V.newViewNum = ?";
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

	private int getLatestSequenceNumber() throws SQLException {
		String query = "SELECT MAX(P.seqNum) FROM Preprepares P";
		PreparedStatement pstmt = logger.getPreparedStatement(query);
		ResultSet ret = pstmt.executeQuery();

		if (ret.next())  //Normal condition
			return ret.getInt(1);
		else            //Initial condition
			return 0;
	}

	private void broadcastToReplica(RequestMessage message) {
		try {
			int seqNum = getLatestSequenceNumber() + 1;

			Operation operation = message.getOperation();
			PreprepareMessage preprepareMessage = makePrePrepareMsg(getPrivateKey(), getPrimary(), seqNum, operation);
			logger.insertMessage(preprepareMessage);

			//Broadcast messages
			replicas.values().forEach(channel -> send(channel, preprepareMessage));
		} catch (SQLException e) {
			e.printStackTrace();
		}
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

	int getMyNumber() {
		return this.myNumber;
	}

	Logger getLogger() {
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
		PublicKey key = publicKeyMap.get(replicas.get(message.getNewViewNum()));
		if (!message.isVerified(key))
			return;

		setViewChangePhase(false);
		if ((message.getNewViewNum() <= this.getViewNum())) {
			if (DEBUG) {
				System.err.printf("Received view number(%d) is less than or equal to current view number(%d)",
						message.getNewViewNum(), this.getViewNum());
			}
			return;
		}
		setViewNum(message.getNewViewNum());

		message.getOperationList()
				.stream()
				.map(pp -> makePrepareMsg(getPrivateKey(), pp.getViewNum(), pp.getSeqNum(), pp.getDigest(), getMyNumber()))
				.forEach(msg -> replicas.values().forEach(sock -> send(sock, msg)));
	}

	public Map<String, Timer> getTimerMap() {
		return timerMap;
	}
}
