package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Blockchain.BlockHeader;
import kr.ac.hongik.apl.Messages.GossipReplyMessage;
import kr.ac.hongik.apl.Messages.GossipRequestMessage;
import kr.ac.hongik.apl.Messages.Message;
import org.apache.commons.lang3.NotImplementedException;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static kr.ac.hongik.apl.Util.deserialize;
import static kr.ac.hongik.apl.Util.serialize;

public class GossipClient {
	private static final String REPLICA = "replica";
	private static final String GOSSIP = "gossip";

	private final Selector selector;
	private final int myNumber;
	private final PublicKey publicKey;
	private final PrivateKey privateKey;
	private final Map<Integer, PublicKey> replicaPublicKeyMap;

	private List<SocketAddress> gossipHandlerAddresses;
	private Map<Integer, SocketChannel> gossipHandlers = new ConcurrentHashMap<>();
	/**
	 * request들을 관리하기 위한 자료구조이다.
	 * key : requestTime
	 * value : verify된 reply를 보낸 GossipHandler의 replicaNumber
	 */
	private Map<Long, Map<String, Integer>> requestTimeMap = new HashMap<>();

	public GossipClient(Properties prop, Map<Integer, PublicKey> replicaPublicKeyMap, int myNumber, PublicKey publicKey, PrivateKey privateKey) {
		this.publicKey = publicKey;
		this.privateKey = privateKey;
		this.myNumber = myNumber;
		this.replicaPublicKeyMap = replicaPublicKeyMap;

		this.gossipHandlerAddresses = parseGossipHandlerAddressFromProperty(prop);

		try {
			this.selector = Selector.open();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		connect();
	}

	private SocketChannel handshake(SocketAddress address) throws IOException {
		SocketChannel channel = SocketChannel.open(address);
		while (!channel.finishConnect()) ;
		channel.configureBlocking(false);
		channel.register(selector, SelectionKey.OP_READ);
		return channel;
	}

	/**
	 * Try handshaking and send my public key
	 */
	private SocketChannel makeConnection(SocketAddress address) throws IOException {
		SocketChannel channel = handshake(address);
		return channel;
	}

	private void connect() {
		SocketChannel socketChannel = null;
		for (int i = 0; i < this.gossipHandlerAddresses.size(); ++i) {
			try {
				socketChannel = makeConnection(gossipHandlerAddresses.get(i));
				gossipHandlers.put(i, socketChannel);
			} catch (IOException ignore) {
			}
		}
	}

	/**
	 * @return f+1개 이상 모인 reply message
	 */
	public Map<String, BlockHeader> getReply() {
		while (true) {
			Message message = receive();
			if (!(message instanceof GossipReplyMessage)) {
				throw new NotImplementedException(String.format("Message type is not GossipReplyMessage. type : %s", message.getClass().getTypeName()));
			}
			GossipReplyMessage replyMessage = (GossipReplyMessage) message;
			int replicaNumber = replyMessage.getReplicaNumber();
			long requestTime = replyMessage.getRequestEpochTime();
			Map<String, BlockHeader> blockHeaders = replyMessage.getLatestHeaders();
			//TODO : verify 활성화 필요
/*
			if (!replyMessage.verify(replicaPublicKeyMap.get(replicaNumber))) {
			System.err.println("verification FAIL");
				continue;
			}
 */
			if (!requestTimeMap.containsKey(requestTime)) {
				System.err.println("requestTime check FAIL");
				continue;
			}
			Map<String, Integer> hashMap = requestTimeMap.get(requestTime);
			String hashedLatestHeaders = hashLatestHeaders(blockHeaders);

			if (hashMap.containsKey(hashedLatestHeaders)) {
				int count = hashMap.get(hashedLatestHeaders);
				hashMap.put(hashedLatestHeaders, count + 1);
			} else {
				hashMap.put(hashedLatestHeaders, 0);
			}
			if (hashMap.get(hashedLatestHeaders) >= (replicaPublicKeyMap.size() - 1) % 3 + 1 && replicaPublicKeyMap.size() >= 4) {
				return blockHeaders;
			}
		}

	}

	public void request() {
		long requestTime = Instant.now().toEpochMilli();
		GossipRequestMessage gossipRequestMessage = GossipRequestMessage.makeGossipRequestMessage(privateKey, myNumber, publicKey, requestTime);
		gossipHandlers.values().forEach(s -> send(s, gossipRequestMessage));
		requestTimeMap.put(requestTime, new HashMap<>());
	}

	private void send(SocketChannel channel, GossipRequestMessage message) {
		if (!channel.isConnected()) {
			return;
		}
		byte[] payload = serialize(message);

		try {
			ByteBuffer byteBuffer = ByteBuffer.allocate(4 + payload.length);
			//default order: big endian
			byteBuffer.putInt(payload.length);
			byteBuffer.put(payload);
			byteBuffer.flip();
			while (byteBuffer.hasRemaining()) {
				int n = channel.write(byteBuffer);
			}
		} catch (IOException e) {
			closeWithoutException(channel);
		}
	}

	/**
	 * If the selector contains any listening socket, acceptOp method must be implemented!
	 * Receive mehtod also handles public key sharing situation
	 *
	 * @return Message
	 */
	protected Message receive() {
		//Selector must not hold acceptable or writable replicas
		while (true) {
			try {
				selector.select();
			} catch (IOException e) {
				Replica.msgDebugger.error(e);
				continue;
			}
			Iterator<SelectionKey> it = selector.selectedKeys().iterator();
			while (it.hasNext()) {
				SelectionKey key = it.next();
				it.remove();    //Remove the key from selected-keys set
				if (key.isAcceptable()) {
					ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
					SocketChannel socketChannel = null;
					try {
						socketChannel = serverSocketChannel.accept();
						socketChannel.configureBlocking(false);
						socketChannel.register(selector, SelectionKey.OP_READ);
					} catch (IOException e) {
						Replica.msgDebugger.error(e);
					}
				} else if (key.isReadable()) {
					SocketChannel channel = (SocketChannel) key.channel();
					//ByteArrayOutputStream doubles its buffer when it is full
					ByteBuffer intBuffer = ByteBuffer.allocate(4);
					try {
						int intReadn = 0;
						while (intReadn < 4 && intReadn > -1) {
							intReadn += channel.read(intBuffer);
						}
						if (intReadn == -1) {
							/* Get end of file */
							closeWithoutException(channel);
							continue;    //continue if the stream reads end-of-stream
						}
						intBuffer.flip();
						int length = intBuffer.getInt();    //Default order: big endian
						byte[] receivedBytes = new byte[length];
						ByteBuffer byteBuffer = ByteBuffer.wrap(receivedBytes);
						int readn = 0;
						while (readn < length) {
							readn += channel.read(byteBuffer);
						}

						Serializable message = deserialize(receivedBytes);

						if (!(message instanceof GossipReplyMessage)) {
							throw new IOException("Not acceptable message type");
						}
						return (Message) message;
					} catch (IOException e) {
						Replica.msgDebugger.warn(e);
						closeWithoutException(channel);
					}
				} else {
					Replica.msgDebugger.error(String.format("Not acceptable and also not readable: %s", key));
				}
			}
		}
	}

	private void closeWithoutException(SocketChannel socketChannel) {
		try {
			socketChannel.close();
			SelectionKey key = socketChannel.keyFor(selector);
			key.cancel();
		} catch (IOException | NullPointerException ignore) {
			//Ignore exception
		}
	}
	private List<SocketAddress> parseGossipHandlerAddressFromProperty(Properties prop) {
		int replicas = Integer.parseInt(prop.getProperty(REPLICA));

		List<SocketAddress> socketAddresses = new ArrayList<>();
		for (int i = 0; i < replicas; i++) {
			String value = prop.getProperty(REPLICA + i + "." + GOSSIP);
			String[] splitedAddr = value.split(":");

			socketAddresses.add(new InetSocketAddress(splitedAddr[0], Integer.parseInt(splitedAddr[1])));
		}
		return socketAddresses;
	}

	private String hashLatestHeaders(Map<String, BlockHeader> latestHeaders) {
		String mapString = latestHeaders.entrySet().stream()
				.sorted(Map.Entry.comparingByKey())
				.map(e -> e.getKey() + ":" + e.getValue().toString())
				.collect(Collectors.joining());

		return Util.hash(mapString.getBytes());
	}
}
