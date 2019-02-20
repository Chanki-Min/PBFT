package kr.ac.hongik.apl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Predicate;

import static com.diffplug.common.base.Errors.rethrow;
import static kr.ac.hongik.apl.Util.fastCopy;
import static kr.ac.hongik.apl.Util.serialize;


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
		try {
			this.publicKeyMap.put(channel.getRemoteAddress(), message.getPublicKey());
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
		} catch (IOException e) {
			System.err.println(e);
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

				logger.insertMessage(rightNextCommitMsg.getSeqNum(), replyMessage);
				SocketAddress destination = getAddressByClientInfo(replyMessage.getClientInfo());

				if (Replica.DEBUG)
					System.err.println("Send to client");
				send(destination, replyMessage);
				//TODO: Close connection
				//TODO: Delete client's public key
				//TODO: When sequence number == highWatermark, go to checkpoint phase and update a new lowWatermark
			} catch (NoSuchElementException e) {
				return;
			}
		}
	}

	private SocketAddress getAddressByClientInfo(PublicKey key) {
		return publicKeyMap.entrySet().stream()
				.filter(x -> x.getValue().equals(key))
				.findFirst().get().getKey();
	}

	private void handlePrepareMessage(PrepareMessage message) {
		PublicKey publicKey = publicKeyMap.get(replicaAddresses.get(message.getReplicaNum()));
		if (message.isVerified(publicKey, this.primary, this::getWatermarks)) {
			logger.insertMessage(message);
		}
		if (message.isPrepared(rethrow().wrap(logger::getPreparedStatement), getMaximumFaulty(), this.myNumber)) {
			CommitMessage commitMessage = new CommitMessage(
					this.getPrivateKey(),
					message.getViewNum(),
					message.getSeqNum(),
					message.getDigest(),
					this.myNumber);
			replicaAddresses.forEach(address -> send(address, commitMessage));
		}
	}

	private void handleRequestMessage(RequestMessage message) {
		if (this.primary == this.myNumber
				&& message.isFirstSent(rethrow().wrap(logger::getPreparedStatement))) {
			logger.insertMessage(message);
			//Enter broadcast phase
			broadcastToReplica(message);
		} else {
			//Relay to primary
			super.send(replicaAddresses.get(primary), message);
		}
	}

	private void handlePreprepareMessage(PreprepareMessage message) {
		PublicKey publicKey = publicKeyMap.get(message.getClientInfo());

		if (message.isVerified(publicKey, this.primary, this::getWatermarks, rethrow().wrap(logger::getPreparedStatement))) {
			logger.insertMessage(message);
		}
		PrepareMessage prepareMessage = new PrepareMessage(
				this.getPrivateKey(),
				message.getViewNum(),
				message.getSeqNum(),
				message.getDigest(),
				this.myNumber);
		replicaAddresses.forEach(address -> send(address, prepareMessage));
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
			PreprepareMessage preprepareMessage = new PreprepareMessage(this.getPrivateKey(), this.primary, seqNum, message.getOperation());
			logger.insertMessage(preprepareMessage);
			//Broadcast messages
			replicaAddresses.parallelStream().forEach(address -> send(address, preprepareMessage));
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}


	@Override
	protected void sendHeaderMessage(SocketChannel channel) throws IOException {
		HeaderMessage headerMessage = new HeaderMessage(this.myNumber, this.publicKey, "replica");
		byte[] bytes = serialize(headerMessage);
		if (DEBUG) {
			System.err.println("send " + bytes.length + "bytes");
		}
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		fastCopy(Channels.newChannel(in), channel);
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
