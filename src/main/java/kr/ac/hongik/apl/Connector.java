package kr.ac.hongik.apl;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Caution: It doesn't handling server's listening socket
 */
abstract class Connector {
    final static int bufferSize = 16 * 1024 * 1024;
    protected List<InetSocketAddress> addresses;
    protected List<SocketChannel> sockets;
    protected Selector selector;

    int numOfReplica;

    public Connector() { }

    public Connector(Properties prop) {
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
    }

    private void makeConnections() {
        //Connect to every replica
        sockets = new ArrayList<>();
        addresses.stream().forEach(x -> sockets.add(makeConnectionOrNull(x)));

        sockets.stream().forEach(socket -> {
            try {
                socket.register(this.selector, SelectionKey.OP_READ);
            } catch (ClosedChannelException e) {
                e.printStackTrace();
            }
        });
    }

    private SocketChannel makeConnectionOrNull(InetSocketAddress address){
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
     * @param address
     * @param message
     */
    protected void send(InetSocketAddress address, Message message){
        ByteBuffer serializedMessage = serialize(message);

        for (SocketChannel socket : sockets) {
            try {
                if (socket.getRemoteAddress().equals(address)) {
                    socket.write(serializedMessage);
                }
            } catch (IOException | NullPointerException e){
                e.printStackTrace();
                //Close previous connection
                try {
                    socket.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
                sockets.remove(socket);
                //Reconnect
                sockets.add(makeConnectionOrNull(address));
            }
        }
    }

    /**
     * This abstract method must be implemented in Replica class to handle Accept situation.
     * @param key
     */
    protected abstract void acceptOp(SelectionKey key);

    /**
     * If the selector contains any listening socket, acceptOp method must be implemented!
     * @return Message
     */
    protected Message receive() {
        //Selector must not hold acceptable or writable sockets
        while(true) {
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
                ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bufferSize);
                socketChannel.read(byteBuffer);

                return deserialize(byteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static ByteBuffer serialize(Message message){
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            ObjectOutputStream outputStream = new ObjectOutputStream(out);
            outputStream.writeObject(message);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ByteBuffer.wrap(out.toByteArray());
    }

    static Message deserialize(ByteBuffer bytes){
        ByteArrayInputStream in = new ByteArrayInputStream(bytes.array());
        Message object = null;
        try {
            ObjectInputStream inputStream = new ObjectInputStream(in);
            object = (Message) inputStream.readObject();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return object;
    }

}
