package kr.ac.hongik.apl;

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

import static com.diffplug.common.base.Errors.rethrow;
import static kr.ac.hongik.apl.PreprepareMessage.makePrePrepareMsg;
import static kr.ac.hongik.apl.ReplyMessage.makeReplyMsg;


public class Replica extends Connector {
	public static final boolean DEBUG = true;

	final static int WATERMARK_UNIT = 100;
	private final static double timeout = 1.;


	private final int myNumber;
	private int primary = 0;

	private ServerSocketChannel listener;
	private List<SocketChannel> clients;
	private PriorityQueue<CommitMessage> priorityQueue = new PriorityQueue<>(Comparator.comparingInt(CommitMessage::getSeqNum));

	private Logger logger;
	private int lowWatermark;


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
			} else if (message instanceof RequestMessage) {
				handleRequestMessage((RequestMessage) message);
			} else if (message instanceof PreprepareMessage) {
				handlePreprepareMessage((PreprepareMessage) message);
			} else if (message instanceof PrepareMessage) {
				handlePrepareMessage((PrepareMessage) message);
			} else if (message instanceof CommitMessage) {
				handleCommitMessage((CommitMessage) message);
			} else if (message instanceof CheckPointMessage){
				handleCheckPointMessage((CheckPointMessage) message);
			}else
				throw new UnsupportedOperationException("Invalid message");
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
	private void handleCheckPointMessage(CheckPointMessage message){
		PublicKey publicKey = publicKeyMap.get(this.replicas.get(message.getReplicaNum()));
		if(message.verify(publicKey)) {
			logger.insertMessage(message);

			String query = "SELECT stateDigest FROM Checkpoint C WHERE C.seqNum = ?";

			try(var psmt = logger.getPreparedStatement(query)) {

				psmt.setInt(1,message.getSeqNum());

				try(var ret = psmt.executeQuery()) {
					List<String> digestList = new ArrayList<>();
					while(ret.next()) {
						digestList.add(ret.getString(1));
					}
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

	private void handlePrepareMessage(PrepareMessage message) {
		PublicKey publicKey = publicKeyMap.get(this.replicas.get(message.getReplicaNum()));
		if (message.isVerified(publicKey, this.primary, this::getWatermarks)) {
			logger.insertMessage(message);
		}
		try(var pstmt = logger.getPreparedStatement("SELECT count(*) FROM Commits C WHERE C.seqNum = ? AND C.replica = ?")) {
			pstmt.setInt(1, message.getSeqNum());
			pstmt.setInt(2, this.myNumber);
			try(var ret = pstmt.executeQuery()) {
				if(ret.next()) {
					var i = ret.getInt(1);
					if(i > 0)
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
			if (this.primary == this.myNumber) {
				//Enter broadcast phase
				broadcastToReplica(message);
			} else {
				//Relay to primary
				super.send(replicas.get(primary), message);
			}
		}
	}

	private void handlePreprepareMessage(PreprepareMessage message) {
		SocketChannel primaryChannel = this.replicas.get(this.primary);
		PublicKey publicKey = this.publicKeyMap.get(primaryChannel);
		boolean isVerified = message.isVerified(publicKey, this.primary, this::getWatermarks, rethrow().wrap(logger::getPreparedStatement));

		if (isVerified) {
			logger.insertMessage(message);
			PrepareMessage prepareMessage = PrepareMessage.makePrepareMsg(
					this.getPrivateKey(),
					message.getViewNum(),
					message.getSeqNum(),
					message.getDigest(),
					this.myNumber);
			replicas.values().forEach(channel -> send(channel, prepareMessage));
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
			PreprepareMessage preprepareMessage = makePrePrepareMsg(getPrivateKey(), primary, seqNum, operation);
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

}
