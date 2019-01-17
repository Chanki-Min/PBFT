package kr.ac.hongik.apl;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class Replica extends Connector implements Primary, Backup {
    static final String path = "src/main/resources/replica.properties";

    static final double timeout = 1.;
    int primary = 0;
    final int myNumber;

    ServerSocketChannel serverSocketChannel;
    List<SocketChannel> clients;

    private Logger logger;


    public Replica(Properties prop, String serverIp, int serverPort) {
        super(prop);

        this.logger = new Logger();

        this.myNumber = getMyNumberFromProperty(prop, serverIp, serverPort);

        InetSocketAddress serverAddress = new InetSocketAddress(serverIp, serverPort);
        try {
            this.serverSocketChannel = ServerSocketChannel.open();
            this.serverSocketChannel.bind(serverAddress);
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            e.printStackTrace();
        }
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

    public static void main(String[] args) throws IOException {
        String ip = "127.0.0.1";
        int port = 0;
        Properties properties = new Properties();
        FileInputStream fis = new FileInputStream(path);
        properties.load(new java.io.BufferedInputStream(fis));

        Replica replica = new Replica(properties, ip, port);
    }

    public void start() {
        //Assume that every connection is established

        while (true) {
            Message message = super.receive();
            if (message instanceof RequestMessage) {
                if (this.primary == this.myNumber) {
                    //Enter broadcast phase
                    startPrepreparePhase((RequestMessage) message);
                } else {
                    //Relay to primary
                    super.send(addresses.get(primary), message);
                }
            }
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

    public void startPrepreparePhase(RequestMessage message) {
        try {
            int seqNum = getLatestSequenceNumber() + 1;
            PreprepareMessage preprepareMessage = new PreprepareMessage(this.getPrivateKey(), this.primary, seqNum, message.getOperation());
            //Broadcast messages
            addresses.parallelStream().forEach(address -> send(address, preprepareMessage));
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
