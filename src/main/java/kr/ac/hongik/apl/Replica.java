package kr.ac.hongik.apl;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Properties;

public class Replica extends Connector implements Primary, Backup {
    static final String path = "src/main/resources/replica.properties";

    static final double timeout = 1.;
    int primary = 0;

    ServerSocketChannel serverSocketChannel;
    List<SocketChannel> clients;



    public Replica(Properties prop, String serverIp, int serverPort){
        super(prop);
        InetSocketAddress serverAddress = new InetSocketAddress(serverIp, serverPort);
        try {
            this.serverSocketChannel =  ServerSocketChannel.open();
            this.serverSocketChannel.bind(serverAddress);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String ip = "127.0.0.1";
        int port = 0;
        Properties properties = new Properties();
        FileInputStream fis = new FileInputStream(path);
        properties.load(new java.io.BufferedInputStream(fis));

        Replica replica = new Replica(properties, ip, port);
    }

    public void broadcastRequest(Message msg) {

    }

    public void broadcastPrePrepare(Message message) {

    }

    public void broadcastPrepare(Message message) {

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

    public void broadcastCheckpoint(Message message) {

    }

    public void broadcastCommit(Message message) {

        //Close connection
    }


}
