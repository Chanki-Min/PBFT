package kr.ac.hongik.apl;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.diffplug.common.base.Errors.rethrow;
import static kr.ac.hongik.apl.Util.sign;
import static kr.ac.hongik.apl.Util.verify;


public class Replica extends Connector {
	public static final boolean DEBUG = true;

	final static int WATERMARK_UNIT = 100;
	private final static double timeout = 1.;


	private final int myNumber;
	private int primary = 0;

	private ServerSocketChannel listener;
	private List<SocketChannel> clients;
	private InetSocketAddress myAddress;
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

		this.myAddress = new InetSocketAddress(serverIp, serverPort);
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
				if (DEBUG) System.err.println("Got Header message");
				handleHeaderMessage((HeaderMessage) message);
			} else if (message instanceof RequestMessage) {
				if (DEBUG) System.err.println("Got request message");
				handleRequestMessage((RequestMessage) message);
			} else if (message instanceof PreprepareMessage) {
				if (DEBUG) System.err.println("Got PrePrepare message");
				handlePreprepareMessage((PreprepareMessage) message);
			} else if (message instanceof PrepareMessage) {
				if (DEBUG) System.err.println("Got Prepare message");
				handlePrepareMessage((PrepareMessage) message);
			} else if (message instanceof CommitMessage) {
				if (DEBUG) System.err.println("Got Commit message");
				handleCommitMessage((CommitMessage) message);
			} else
				throw new UnsupportedOperationException("Invalid message");
		}
	}

	private void handleHeaderMessage(HeaderMessage message) {
		SocketChannel channel = message.getChannel();
		this.publicKeyMap.put(channel, message.getPublicKey());

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

	private void handleCommitMessage(CommitMessage cmsg) {
		Predicate<CommitMessage> committed = (x) -> x.isCommittedLocal(rethrow().wrap(logger::getPreparedStatement),
				getMaximumFaulty(), this.myNumber);

		logger.insertMessage(cmsg);

		if (committed.test(cmsg)) {
			priorityQueue.add(cmsg);
			try {
				CommitMessage rightNextCommitMsg = getRightNextCommitMsg();
				var operation = logger.getOperation(rightNextCommitMsg);
				Result ret = operation.execute();
				ReplyMessage replyMessage = new ReplyMessage(
						getPrivateKey(),
						cmsg.getViewNum(),
						operation.getTimestamp(),
						operation.getClientInfo(),
						this.myNumber,
						ret);


				//TODO (Technical Debt): PrePrepare과 동일한 문제
				var sig = Util.sign(this.getPrivateKey(), replyMessage.getData());
				if(!sig.equals(replyMessage.getSignature())) {
					replyMessage.setSignature(sig);
				}
				if (!replyMessage.verifySignature(this.getPublicKey())) throw new AssertionError();

				logger.insertMessage(rightNextCommitMsg.getSeqNum(), replyMessage);
				SocketChannel destination = getChannelFromClientInfo(replyMessage.getClientInfo());

				send(destination, replyMessage);
				//TODO: Close connection
				//TODO: Delete client's public key
				//TODO: When sequence number == highWatermark, go to checkpoint phase and update a new lowWatermark
			} catch (NoSuchElementException e) {
				return;
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
		if(Replica.DEBUG){
			System.err.println("Get into handlePrepareMessage");
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
			if(Replica.DEBUG){
				System.err.println("isPrepared Passed");
			}
			CommitMessage commitMessage = new CommitMessage(
					this.getPrivateKey(),
					message.getViewNum(),
					message.getSeqNum(),
					message.getDigest(),
					this.myNumber);
			if (Replica.DEBUG && this.myNumber != commitMessage.getReplicaNum()) {
                System.err.printf("replica: %d, written: %d, port: %d\n", this.myNumber, message.getReplicaNum(), this.myAddress.getPort());
                throw new AssertionError();
			}
			if(Replica.DEBUG){
				System.err.println(this.myAddress + " : " + commitMessage.getReplicaNum()+ " : "+ this.myNumber);
			}

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
		if (!publicKeyMap.containsValue(message.getClientInfo())) throw new AssertionError();
		Supplier<Boolean> check = () ->
                message.verify(message.getClientInfo()) &&
				message.isFirstSent(rethrow().wrap(logger::getPreparedStatement));


		if (this.primary == this.myNumber && check.get()) {
			logger.insertMessage(message);
			//Enter broadcast phase
			broadcastToReplica(message);
		} else {
			//Relay to primary
			super.send(replicas.get(primary), message);
		}
	}

	private void handlePreprepareMessage(PreprepareMessage message) {
		SocketChannel primaryChannel = this.replicas.get(this.primary);
		PublicKey publicKey = this.publicKeyMap.get(primaryChannel);
		if(Replica.DEBUG && this.primary == this.myNumber) {
			if (!this.getPublicKey().equals(publicKey)) throw new AssertionError();
		}
		Supplier<Boolean> check = () -> message.isVerified(publicKey, this.primary, this::getWatermarks, rethrow().wrap(logger::getPreparedStatement));
		if (check.get()) {
			logger.insertMessage(message);
			PrepareMessage prepareMessage = new PrepareMessage(
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
			PreprepareMessage preprepareMessage = new PreprepareMessage(
					this.getPrivateKey(),
					this.primary,
					seqNum,
					message.getOperation()
			);
			logger.insertMessage(preprepareMessage);

			var sig = Util.sign(this.getPrivateKey(), preprepareMessage.getData());
			//TODO(Technical Debt): 왜 같은 시그니처를 같다고 하지 못하는지 모르겠지만, 임시로 해결함
			if (!sig.equals(preprepareMessage.getSignature())) {
				preprepareMessage.setSignature(sig);
			}


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

	public void broadcastViewChange(Message message) {

	}

	public void broadcastNewView(Message message) {

	}
}
