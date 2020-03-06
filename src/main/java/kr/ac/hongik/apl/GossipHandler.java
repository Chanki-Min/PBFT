package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Blockchain.BlockHeader;
import kr.ac.hongik.apl.Messages.GossipReplyMessage;
import kr.ac.hongik.apl.Messages.GossipRequestMessage;
import kr.ac.hongik.apl.Messages.Message;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static kr.ac.hongik.apl.Util.deserialize;
import static kr.ac.hongik.apl.Util.serialize;

public class GossipHandler {
	private static final String REPLICA = "replica";
	private static final String GOSSIP = "gossip";

	private final Selector selector;
	private final int myNumber;
	private final Logger logger;
	private final PublicKey publicKey;
	private final PrivateKey privateKey;


	public GossipHandler(int myNumber, Properties prop, Logger logger, PublicKey publicKey, PrivateKey privateKey) {
		this.myNumber = myNumber;
		this.logger = logger;
		this.publicKey = publicKey;
		this.privateKey = privateKey;
		int gossipPort = parseGossipPortFromProperty(myNumber, prop);

		try {
			this.selector = Selector.open();
			ServerSocketChannel listener = ServerSocketChannel.open();
			listener.socket().setReuseAddress(true);
			listener.configureBlocking(false);
			listener.bind(new InetSocketAddress(gossipPort));
			listener.register(selector, SelectionKey.OP_ACCEPT);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void start() {
		Replica.msgDebugger.info("GossipHandler READY");
		while (true) {
			Message message = receive();
			if ((message instanceof GossipRequestMessage)) {
				handleGossipMessage((GossipRequestMessage) message);
			} else {
				Replica.msgDebugger.error(String.format("Invalid message type, %s is not acceptable by GossipHandler", message.getClass().getTypeName()));
			}
		}
	}

	private void handleGossipMessage(GossipRequestMessage message) {
		//TODO : message.verify()는 각 replica들이 고정된 private key를 가져야 성립된다
		Replica.detailDebugger.debug(String.format("Got GossipRequestMsg from %s", message.socketChannel));
		Map<String, BlockHeader> latestHeaders = logger.getLatestHeaders();
		GossipReplyMessage replyMessage = GossipReplyMessage.makeGossipReplyMessage(privateKey, myNumber, latestHeaders, message.clientPublicKey, message.requestTime);
		send(message.socketChannel, replyMessage);
	}

	private int parseGossipPortFromProperty(int myNumber, Properties prop) {
		String propertyName = getGossipPropertyName(myNumber);
		String value = prop.getProperty(propertyName);
		return Integer.parseInt(value.split(":")[2]);
	}

	private void send(SocketChannel channel, Message message) {
		if(!channel.isConnected()) {
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
						if(message instanceof GossipRequestMessage) {
							((GossipRequestMessage) message).socketChannel = channel;
						} else {
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

	protected void closeWithoutException(SocketChannel socketChannel) {
		try {
			socketChannel.close();
			SelectionKey key = socketChannel.keyFor(selector);
			key.cancel();
		} catch (IOException | NullPointerException ignore) {
			//Ignore exception
		}
	}

	private String getGossipPropertyName(int gossipHandlerNum) {
		return REPLICA + gossipHandlerNum + "." + GOSSIP;
	}
}
