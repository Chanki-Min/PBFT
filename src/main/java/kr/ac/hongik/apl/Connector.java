package kr.ac.hongik.apl;


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
 * Caution: It doesn't handle server's listening socket
 */
abstract class Connector {
	//Invariant: replica index and its socket is matched!
	protected List<InetSocketAddress> replicaAddresses = new ArrayList<>();
	protected Map<Integer, SocketChannel> replicas = new HashMap<>();
	protected Selector selector;

	protected Map<SocketAddress, PublicKey> publicKeyMap = new HashMap<>();
	private PrivateKey privateKey;            //Don't try to access directly, instead access via getter
	protected PublicKey publicKey;

	int numOfReplica;

	public Connector(Properties prop) {
		KeyPair keyPair = generateKeyPair();
		this.privateKey = keyPair.getPrivate();
		this.publicKey = keyPair.getPublic();
		try {
			this.selector = Selector.open();
		} catch (IOException e) {
			e.printStackTrace();
		}
		parseProperties(prop);

	}

	protected void connect()  {
		//Connect to every replica
		SocketChannel socketChannel = null;
		for (int i = 0; i < this.replicaAddresses.size(); ++i) {
			try {
				socketChannel = makeConnection(replicaAddresses.get(i));
				this.replicas.put(i, socketChannel);
			} catch(IOException e) {
				closeWithoutException(socketChannel);
				continue;
			}
		}
	}

	private void parseProperties(Properties prop) {
		numOfReplica = Integer.parseInt(prop.getProperty("replica"));

		for (int i = 0; i < numOfReplica; i++) {
			String addressInString = prop.getProperty("replica" + i);
			String[] parsedAddress = addressInString.split(":");

			InetSocketAddress address = new InetSocketAddress(parsedAddress[0], Integer.parseInt(parsedAddress[1]));
			replicaAddresses.add(address);
		}
	}

	private void closeWithoutException(SocketChannel socketChannel) {
		try {
			socketChannel.close();
		} catch (IOException e) {
			//e.printStackTrace();
			System.err.println(e);
		} catch (NullPointerException e) {
			//Ignore error message
		}
	}

	protected abstract void sendHeaderMessage(SocketChannel channel) throws IOException;

	private SocketChannel handshake(SocketAddress address) throws IOException {
		SocketChannel channel = SocketChannel.open(address);
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

	/**
	 * It is a exception free wrapper of send function.
	 * It sends the data and also manage the wrong replicas
	 * The endpoint object must be in the replicas list, or this function does nothing.
	 * <p>
	 * Plus, the order of replicas are maintained while reconnecting
	 *
	 * @param destination
	 * @param message
	 */
	protected void send(SocketAddress destination, Message message) {
		byte[] payload = serialize(message);

		Iterator<SocketChannel> it = selector.selectedKeys()
				.stream()
				.map(x -> (SocketChannel) x.channel())
				.iterator();
		while(it.hasNext()) {
			SocketChannel channel = it.next();
			try{
				if (channel.getRemoteAddress().equals(destination)) {
					ByteBuffer byteBuffer = ByteBuffer.allocate(4 + payload.length);
					//default order: big endian
					byteBuffer.putInt(payload.length);
					byteBuffer.put(payload);

					channel.write(byteBuffer);
				}
			} catch(IOException e) {
				/* Broken connection: try reconnecting if that socket was connected to replica */
				replicas.remove(channel);
				try {
					int replicaNum = replicas.entrySet().stream()
							.filter(x -> x.getValue().equals(channel))
							.findFirst().orElseThrow(NoSuchElementException::new).getKey();
					closeWithoutException(channel);    //de-register a selector
					var address = replicaAddresses.get(replicaNum);
					try {
						SocketChannel newChannel = makeConnection(address);
						replicas.put(replicaNum, newChannel);
					} catch (IOException e1) {
						//pass
					}

				} catch (NoSuchElementException e1) {
					//client socket is failed. ignore reconnection
					closeWithoutException(channel);    //de-register a selector
					return;
				}
				/*
				 * 재전송을 안하는 이유:
				 * getRemoteAddress에서 exception 발생시 재전송하게 되면 원치 않는 곳에 전송할 수 있기 때문
				 */
				//clients?
			}
		}
	}

	/**
	 * This abstract method must be implemented in Replica class to handle Accept situation.
	 *
	 * @param key
	 */
	protected abstract void acceptOp(SelectionKey key);

	/**
	 * If the selector contains any listening socket, acceptOp method must be implemented!
	 * Receive mehtod also handles public key sharing situation
	 * @return Message
	 */
	protected Message receive() {
		//Selector must not hold acceptable or writable replicas
		//TODO: Consider closing the socket situation
		while (true) {
			try {
				selector.select();
				if (Replica.DEBUG)
					System.err.println("Selected");
				Iterator<SelectionKey> it = selector.selectedKeys().iterator();
				while(it.hasNext()) {
					SelectionKey key = it.next();
					it.remove();    //Remove the key from selected-keys set
					if(key.isAcceptable()){
						ServerSocketChannel serverSocketChannel = (ServerSocketChannel)key.channel();
						SocketChannel socketChannel = serverSocketChannel.accept();
						socketChannel.configureBlocking(false);
						socketChannel.register(selector, SelectionKey.OP_READ);
						this.sendHeaderMessage(socketChannel);
					}
					else if(key.isReadable()){
						SocketChannel channel = (SocketChannel) key.channel();
						//ByteArrayOutputStream doubles its buffer when it is full
						ByteBuffer intBuffer = ByteBuffer.allocate(4);
						int n = channel.read(intBuffer);
						if(n != 4) {
							if(Replica.DEBUG) {
								System.err.println("read returns " + n);
							}
							continue;	//continue if stream read end-of-stream
						}
						intBuffer.flip();
						int length = intBuffer.getInt();    //Default order: big endian
						if(length == 0) {
							//continue;	//continue if stream read end-of-stream
						}
						byte[] receivedBytes = new byte[length];
						ByteBuffer byteBuffer = ByteBuffer.wrap(receivedBytes);
						int reads = channel.read(byteBuffer);
						if(Replica.DEBUG){
							System.err.printf("Expected %d, receive %d bytes\n", length, reads);
							assert reads == length;
							assert length == byteBuffer.position();
						}
						Serializable message = deserialize(receivedBytes);
						if (message instanceof HeaderMessage) {
							HeaderMessage headerMessage = (HeaderMessage)message;
							headerMessage.setChannel(channel);
						}
						return (Message) message;
					}
					else{
						System.err.println(key.toString());
					}

				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/************************************************************
	 *
	 * This methods are just a common methods, not related to connection
	 */

	protected final PrivateKey getPrivateKey() {
		return this.privateKey;
	}

	public PublicKey getPublicKey() {
		return this.publicKey;
	}

	final int getMaximumFaulty() {
		return this.numOfReplica / 3;
	}

}
