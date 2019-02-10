package kr.ac.hongik.apl;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.*;
import java.util.stream.Collectors;

import static kr.ac.hongik.apl.Util.*;


/**
 * Caution: It doesn't handle server's listening socket
 */
abstract class Connector {
	//Invariant: replica index and its socket is matched!
	protected List<InetSocketAddress> addresses;
	protected List<SocketChannel> sockets;
	protected Selector selector;

	protected Map<InetSocketAddress, PublicKey> publicKeyMap;
	private PrivateKey privateKey;            //Don't try to access directly, instead access via getter
	protected PublicKey publicKey;

	int numOfReplica;

	static class PublicKeyMessage implements Message {
		public PublicKey publicKey;

		public PublicKeyMessage(PublicKey publicKey) {
			this.publicKey = publicKey;
		}
	}

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

	protected void connect() {
		//Connect to every replica
		this.sockets = this.addresses.stream()
				.map(this::makeConnectionOrNull)
				.collect(Collectors.toList());
	}

	private void parseProperties(Properties prop) {
		numOfReplica = Integer.parseInt(prop.getProperty("replica"));

		addresses = new ArrayList<>();
		for (int i = 0; i < numOfReplica; i++) {
			String addressInString = prop.getProperty("replica" + i);
			String[] parsedAddress = addressInString.split(":");

			InetSocketAddress address = new InetSocketAddress(parsedAddress[0], Integer.parseInt(parsedAddress[1]));
			addresses.add(address);
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

    protected void sendPublicKey(SocketChannel channel) throws IOException {
		byte[] bytes = serialize(new PublicKeyMessage(this.publicKey));
		channel.write(ByteBuffer.wrap(bytes));
	}

	private SocketChannel handshake(InetSocketAddress address) throws IOException {
		SocketChannel channel = SocketChannel.open(address);
		channel.configureBlocking(false);
		//channel.socket().setReceiveBufferSize(Connector.BUFFER_SIZE);
		//channel.socket().setSendBufferSize(Connector.BUFFER_SIZE);
		channel.register(selector, SelectionKey.OP_READ);
		return channel;
	}

	/**
	 * Try handshaking and send my public key
	 */
	private SocketChannel makeConnectionOrNull(InetSocketAddress address) {
		SocketChannel channel = null;
		try {
			channel = handshake(address);
			sendPublicKey(channel);
			return channel;
		} catch (IOException | NullPointerException e) {
			//e.printStackTrace();
			System.err.printf("%s from %s\n", e, this);
			closeWithoutException(channel);
			return null;
		}
	}

	/**
	 * It is a exception free wrapper of send function.
	 * It sends the data and also manage the wrong sockets
	 * The endpoint object must be in the sockets list, or this function does nothing.
	 * <p>
	 * Plus, the order of sockets are maintained while reconnecting
	 *
	 * @param destination
	 * @param message
	 */
	protected void send(InetSocketAddress destination, Message message) {
		byte[] bytes = serialize(message);
		InputStream in = new ByteArrayInputStream(bytes);

		if (sockets == null) throw new AssertionError();
		sockets = sockets.stream().map(channel -> {
			try {
				if (channel.getRemoteAddress().equals(destination)) {
					fastCopy(Channels.newChannel(in), channel);
				}
				return channel;
			} catch (IOException | NullPointerException e) {
				//e.printStackTrace();
				closeWithoutException(channel);
				return makeConnectionOrNull(destination);
			}
		}).collect(Collectors.toList());
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
		//Selector must not hold acceptable or writable sockets
		//TODO: Consider closing the socket situation
		while (true) {
			try {
				selector.select();
				Iterator<SelectionKey> it = selector.selectedKeys().iterator();
				SelectionKey key = it.next();
				it.remove();    //Remove the key from selected-keys set
				if (key.isAcceptable()) {
					acceptOp(key);
					continue;
				}
				//Invariant: every key must be readable
				SocketChannel channel = (SocketChannel) key.channel();
				//ByteArrayOutputStream doubles its buffer when it is full
				ByteArrayOutputStream os = new ByteArrayOutputStream();
				fastCopy(channel, Channels.newChannel(os));

				Message message = (Message) deserialize(os.toByteArray());
				if (message instanceof PublicKeyMessage) {
					this.publicKeyMap.put(
							(InetSocketAddress) channel.getRemoteAddress(),
							((PublicKeyMessage) message).publicKey);
					continue;
				}
				return message;
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
