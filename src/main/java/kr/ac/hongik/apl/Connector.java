package kr.ac.hongik.apl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.*;
import java.util.stream.Collectors;

import static kr.ac.hongik.apl.Util.*;

class PublicKeyMessage implements Message {
	public PublicKey publicKey;

	public PublicKeyMessage(PublicKey publicKey) {
		this.publicKey = publicKey;
	}
}

/**
 * Caution: It doesn't handling server's listening socket
 */
abstract class Connector {
	final static int BUFFER_SIZE = 16 * 1024 * 1024;
	//Invariant: replica index and its socket is matched!
	protected List<InetSocketAddress> addresses;
	protected List<SocketChannel> sockets;
	protected Selector selector;

	protected Map<InetSocketAddress, PublicKey> publicKeyMap;
	private PrivateKey privateKey;            //Don't try to access directly, instead access via getter



	int numOfReplica;

	public Connector(Properties prop) {
		KeyPair keyPair = generateKeyPair();
		this.privateKey = keyPair.getPrivate();
		PublicKey publicKey = keyPair.getPublic();

		numOfReplica = Integer.parseInt(prop.getProperty("replica"));

		addresses = new ArrayList<>();
		for (int i = 0; i < numOfReplica; i++) {
			String addressInString = prop.getProperty("replica" + i);
			String[] parsedAddress = addressInString.split(":");

			InetSocketAddress address = new InetSocketAddress(parsedAddress[0], Integer.parseInt(parsedAddress[1]));
			addresses.add(address);
		}

		try {
			this.selector = Selector.open();
		} catch (IOException e) {
			e.printStackTrace();
		}

		makeConnections();
		//TODO: Invariant: every connection must be established!
		broadcastPublicKey(publicKey);
	}

	private void broadcastPublicKey(PublicKey publicKey) {
		PublicKeyMessage publicKeyMessage = new PublicKeyMessage(publicKey);
		for (var address : addresses) {
			send(address, publicKeyMessage);
		}
	}

	protected final PrivateKey getPrivateKey() {
		return this.privateKey;
	}

	private void makeConnections() {
		//Connect to every replica
		sockets = addresses.stream()
				.map(address -> {
					var socket = makeConnectionOrNull(address);
					try {
						socket.register(this.selector, SelectionKey.OP_READ);
					} catch (ClosedChannelException | NullPointerException e) {
						e.printStackTrace();
					}
					return socket;
				}).collect(Collectors.toList());
	}

	private void closeWithoutException(SocketChannel socketChannel) {
		try {
			socketChannel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private SocketChannel makeConnectionOrNull(InetSocketAddress address) {
		try {
			SocketChannel socket = SocketChannel.open(address);
			return socket;
		} catch (IOException e) {
			e.printStackTrace();
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
		ByteBuffer serializedMessage = ByteBuffer.wrap(serialize(message));

		sockets = sockets.stream().map(socket -> {
			try {
				if (socket.getRemoteAddress().equals(destination))
					socket.write(serializedMessage);
				return socket;
			} catch (IOException | NullPointerException e) {
				e.printStackTrace();
				closeWithoutException(socket);
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
				SocketChannel socketChannel = (SocketChannel) key.channel();
				ByteBuffer byteBuffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
				socketChannel.read(byteBuffer);

				Message message = (Message) deserialize(byteBuffer.array());
				if (message instanceof PublicKeyMessage) {
					this.publicKeyMap.put(
							(InetSocketAddress) socketChannel.getRemoteAddress(),
							((PublicKeyMessage) message).publicKey);
					continue;
				}
				return message;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


}
