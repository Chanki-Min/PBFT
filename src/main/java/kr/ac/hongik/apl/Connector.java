package kr.ac.hongik.apl;


import kr.ac.hongik.apl.Messages.CloseConnectionMessage;
import kr.ac.hongik.apl.Messages.HeaderMessage;
import kr.ac.hongik.apl.Messages.HeartBeatMessage;
import kr.ac.hongik.apl.Messages.Message;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.*;

import static kr.ac.hongik.apl.Util.*;


/**
 * 패킷을 받아서 Message 객체로 Deserialize, send, connect 등의 작업을 수행하는 추상 클래스
 *
 * 주의 : 이를 상속한 클래스가 오픈한 소캣을 바인딩 해줘야 정상적으로 동작할 수 있음
 */
abstract class Connector {
	public final static long TIMEOUT = 15000;    //Unit: milliseconds
	static private Map<Integer, SocketChannel> replicas = new HashMap<>();
	//Invariant: replica index and its socket is matched!
	protected List<InetSocketAddress> replicaAddresses;
	protected Selector selector;

	protected Map<SocketChannel, PublicKey> publicKeyMap = new HashMap<>();
	protected PublicKey publicKey;
	private PrivateKey privateKey;            //Don't try to access directly, instead access via getter


	/**
	 * 객체의 생성자, public-private key를 생성하고 JAVA NIO의 selector를 open, replica의 정보를 Map에 로드한다
	 *
	 * @param prop
	 */
	public Connector(Properties prop) {
		KeyPair keyPair = generateKeyPair();
		this.privateKey = keyPair.getPrivate();
		this.publicKey = keyPair.getPublic();
		try {
			this.selector = Selector.open();
		} catch (IOException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
		replicaAddresses = Util.parseProperties(prop);
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

	protected abstract void sendHeaderMessage(SocketChannel channel) throws IOException;

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
		sendHeaderMessage(channel);

		return channel;
	}

	protected void connect() {
		//Connect to every replica
		SocketChannel socketChannel = null;
		for (int i = 0; i < this.replicaAddresses.size(); ++i) {
			try {
				socketChannel = makeConnection(replicaAddresses.get(i));
				getReplicaMap().put(i, socketChannel);
				Replica.msgDebugger.debug(String.format("HeaderMessage sent to %d", i));
			} catch (IOException e) {
				reconnect(socketChannel);
			}
		}
	}

	/**
	 * It is a exception free wrapper of send function.
	 * It sends the data and also manage the wrong replicas
	 * The endpoint object must be in the replicas list, or this function does nothing.
	 * <p>
	 * Plus, the order of replicas are maintained while reconnecting
	 *
	 * @param channel
	 * @param message
	 */
	public void send(SocketChannel channel, Message message) {
		if(!channel.isConnected()) {
			reconnect(channel);
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
			reconnect(channel);
		}
	}

	/**
	 * send, receive에서 소켓 전송,읽기 작업 실패(IOException)시 호출되는 메소드이다.
	 * 연결에 실패한 소캣채널로 replica id를 찾고, 해당 replica id에 따른 ip:port로 새로운
	 * 소캣채널 연결을 시도한다.
	 *
	 * @param channel 연결에 실패한 소캣채널
	 */
	private void reconnect(SocketChannel channel) {
		/* Broken connection: try reconnecting if that socket was connected to replica */
		Replica.detailDebugger.trace(String.format("try to reconnect with socket %s", channel == null ? "" : channel.toString()));
		SocketChannel newChannel = null;
		try {
			int replicaNum = getReplicaMap().entrySet().stream()
					.filter(x -> x.getValue().equals(channel))
					.findFirst().orElseThrow(NoSuchElementException::new).getKey();
			closeWithoutException(channel);    //de-register a selector
			var address = replicaAddresses.get(replicaNum);
			newChannel = makeConnection(address);
			getReplicaMap().put(replicaNum, newChannel);
		} catch (NoSuchElementException e) {
			//client socket is failed. ignore reconnection
			Replica.msgDebugger.warn(String.format("no matching replicaNum"));
			Replica.msgDebugger.warn(e);
			closeWithoutException(channel);    //de-register a selector
			return;
		} catch (IOException e) {
			Replica.msgDebugger.warn(String.format("reconnection failed"));
			Replica.msgDebugger.warn(e);
			return;
		}
		if(!channel.isConnected()) {
			Replica.msgDebugger.warn(String.format("reconnection failed, serverSocket connection failure"));
		}else {
			Replica.detailDebugger.trace(String.format("reconnected with address : %s", newChannel == null ? "" : newChannel.toString()));
		}
	}

	/**
	 * If the selector contains any listening socket, acceptOp method must be implemented!
	 * Receive mehtod also handles public key sharing situation
	 *
	 * @return Message
	 */
	protected Message receive() throws InterruptedException {
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
						this.sendHeaderMessage(socketChannel);
					} catch (IOException e) {
						reconnect(socketChannel);
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
						if (message instanceof HeaderMessage) {
							HeaderMessage headerMessage = (HeaderMessage) message;
							headerMessage.setChannel(channel);
						} else if (message instanceof CloseConnectionMessage) {
							CloseConnectionMessage closeConnectionMessage = (CloseConnectionMessage) message;
							closeConnectionMessage.setChannel(channel);
						} else if (message instanceof HeartBeatMessage) {
							Replica.detailDebugger.trace("Got heartbeat signal");
							continue;
						}
						return (Message) message;
					} catch (IOException e) {
						reconnect(channel);
					}
				} else {
					Replica.msgDebugger.error(String.format("Not acceptable and also not readable: %s", key));
				}

			}
		}
	}

	static public Map<Integer, SocketChannel> getReplicaMap() {
		return replicas;
	}

	public PublicKey getPublicKey() {
		return this.publicKey;
	}

	public final int getMaximumFaulty() {
		return this.replicaAddresses.size() / 3;
	}

	/************************************************************
	 *
	 * This methods are just a common methods, not related to connection
	 */

	public final PrivateKey getPrivateKey() {
		return this.privateKey;
	}

	public Map<SocketChannel, PublicKey> getPublicKeyMap() {
		return publicKeyMap;
	}
}
