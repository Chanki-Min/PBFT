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
import java.util.stream.Collectors;

import static com.diffplug.common.base.Errors.rethrow;
import static kr.ac.hongik.apl.PrepareMessage.makePrepareMsg;
import static kr.ac.hongik.apl.PreprepareMessage.makePrePrepareMsg;
import static kr.ac.hongik.apl.ReplyMessage.makeReplyMsg;


public class Replica extends Connector {
	public static final boolean DEBUG = true;

	final static int WATERMARK_UNIT = 100;
	private final static long timeout = 5000;    //Unit: milliseconds


	private final int myNumber;
	private int viewNum = 0;

	private ServerSocketChannel listener;
	private List<SocketChannel> clients;
	private PriorityQueue<CommitMessage> priorityQueue = new PriorityQueue<>(Comparator.comparingInt(CommitMessage::getSeqNum));

	private Logger logger;
	private int lowWatermark;

	private Map<String, Timer> timerMap = new HashMap<>();
	private boolean isViewChangePhase = false;    //TODO: Synchronize가 필요할까?, 언제 flag를 해제할까?


	Replica(Properties prop, String serverIp, int serverPort) {
		super(prop);

		String loggerFileName = String.format("consensus_%s_%d.db", serverIp, serverPort);

		this.logger = new Logger(loggerFileName);
		this.clients = new ArrayList<>();

		this.myNumber = getMyNumberFromProperty(prop, serverIp, serverPort);
		this.lowWatermark = 0;

		try {
			listener = ServerSocketChannel.open();
			//listener.socket().setReuseAddress(true);
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
			Message message = super.receive();              //Blocking method
			if (message instanceof HeaderMessage) {
				handleHeaderMessage((HeaderMessage) message);
			} else if (!isViewChangePhase && message instanceof RequestMessage) {
				handleRequestMessage((RequestMessage) message);
			} else if (!isViewChangePhase && message instanceof PreprepareMessage) {
				handlePreprepareMessage((PreprepareMessage) message);
			} else if (!isViewChangePhase && message instanceof PrepareMessage) {
				handlePrepareMessage((PrepareMessage) message);
			} else if (!isViewChangePhase && message instanceof CommitMessage) {
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

	private void handleRequestMessage(RequestMessage message) {
		try {
			if (this.logger.findMessage(message)) {
				return;
			}
		} catch (SQLException e) {
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
				timer.schedule(viewChangeTimerTask, Replica.timeout);

				/* Store timer object to cancel it when the request is executed and the timer is not expired.
				 * key: operation, value: timer
				 * An operation can be a key because every operation has a random UUID;
				 */
				String key = Util.hash(message.getOperation());
				timerMap.put(key, timer);
			}
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
			 * 이미 합의가 이루어져서 reply가 되었다면, 더 이상 진행하지 않고 함수를 종료한다.
			 *
			 * 단, 추후 checkpoint phase를 추가할 시, 로직에 있어서 수정이 필요할 수 있다.
			 */
			if (isAlreadyExecuted(cmsg.getSeqNum()))
				return;

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

				try {
					String key = makeKeyForTimer(rightNextCommitMsg);
					Timer timer = timerMap.remove(key);
					timer.cancel();
				} catch (SQLException e) {
					e.printStackTrace();
				}


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

			} catch (NoSuchElementException e) {
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

			String query = "SELECT stateDigest FROM Checkpoint C WHERE C.seqNum = ?";

			try(var psmt = logger.getPreparedStatement(query)) {

				psmt.setInt(1,message.getSeqNum());

				try (ResultSet ret = psmt.executeQuery()) {
					List<String> digestList = JdbcUtils.toStream(ret)
							.map(rethrow().wrapFunction(x -> x.getString(1)))
							.collect(Collectors.toList());

					int f = getMaximumFaulty();
					//2f + 1개 이상의 메시지를 수집하고,
					//2f + 1개의 메시지 중 f + 1개는 확실히 non-faulty이므로 다들 같은 메시지를 보낼 것이다.
					//나머지 f개는 최악의 경우 전부 faulty 이고, 각자 다른 메시지를 보낼 수 있다.
					//따라서 최악의 경우에는 f + 1개의 서로 다른 메시지가 올 것이다.
					if(digestList.size() > 2 * f && digestList.stream().distinct().count() <= f + 1){
						logger.executeGarbageCollection(message.getSeqNum());
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

		if (!message.verify(publicKey) || message.getNewViewNum() != getMyNumber())
			return;

		logger.insertMessage(message);
		try {
			if (canMakeNewViewMessage(message)) { /* 정확히 2f + 1개일 때만 broadcast */
				NewViewMessage newViewMessage = NewViewMessage.makeNewViewMessage(this, this.getViewNum() + 1);

				replicas.values().forEach(sock -> send(sock, newViewMessage));
			}
		} catch (SQLException e) {
			e.printStackTrace();
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

	public void setViewChangePhase(boolean viewChangePhase) {
		isViewChangePhase = viewChangePhase;
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
		//TODO: 로직 전면적 수정 필요
		PublicKey key = publicKeyMap.get(replicas.get(message.getNewViewNum()));
		if (!message.verify(key))
			return;

		setViewNum(message.getNewViewNum());

		List<ViewChangeMessage> viewChangeMessages = message.getViewChangeMessageList();
		List<PreprepareMessage> receivedMsgs = viewChangeMessages
				.stream()
				.flatMap(x -> x.getMessageList().stream())
				.map(x -> x.getPreprepareMessage())
				.collect(Collectors.toList());
		List<PreprepareMessage> preprepareMessages = message.getOperationList();

		for (var pre_prepareMsg : preprepareMessages) {
			if (receivedMsgs.stream().anyMatch(x -> x.equals(pre_prepareMsg))) {
				PrepareMessage newMsg = makePrepareMsg(getPrivateKey(), message.getNewViewNum(),
						pre_prepareMsg.getSeqNum(), pre_prepareMsg.getDigest(), this.getMyNumber());
				replicas.values().forEach(sock -> send(sock, newMsg));
			}
		}
	}

	public Map<String, Timer> getTimerMap() {
		return timerMap;
	}
}
