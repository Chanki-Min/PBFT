package kr.ac.hongik.apl;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.diffplug.common.base.Errors.rethrow;

public class Replica extends Connector implements Primary, Backup {
    final static String path = "src/main/resources/replica.properties";
    final static int WATERMARK_UNIT = 100;
    private final static double timeout = 1.;


    int primary = 0;
    final int myNumber;

    ServerSocketChannel listener;
    List<SocketChannel> clients;
    private InetSocketAddress myAddress;

    private Logger logger;
    private int lowWatermark;


    public Replica(Properties prop, String serverIp, int serverPort) {
        super(prop);

        this.logger = new Logger();
        this.clients = new ArrayList<>();

        this.myNumber = getMyNumberFromProperty(prop, serverIp, serverPort);
        this.lowWatermark = 0;

        this.myAddress = new InetSocketAddress(serverIp, serverPort);
        try {
            listener = ServerSocketChannel.open();
            listener.socket().setReuseAddress(true);
            listener.configureBlocking(true);
            listener.bind(new InetSocketAddress(serverPort));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Thread acceptanceThread = new Thread(() -> {
            while (true) {
                try {
                    SocketChannel channel = listener.accept();

                    clients.add(channel);
                    channel.configureBlocking(false);
                    channel.register(this.selector, SelectionKey.OP_READ);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        acceptanceThread.setDaemon(true);
        acceptanceThread.start();
    }

    public static void main(String[] args) throws IOException {
        String ip = "127.0.0.1";
        int port = 0;
        Properties properties = new Properties();
        FileInputStream fis = new FileInputStream(path);
        properties.load(new java.io.BufferedInputStream(fis));

        Replica replica = new Replica(properties, ip, port);
        replica.connect();
        replica.start();
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

    public void start() {
        //Assume that every connection is established
        while (true) {
            Message message = super.receive();              //Blocking method
            if (message instanceof RequestMessage) {
                RequestMessage rmsg = (RequestMessage) message;
                if (this.primary == this.myNumber
                        && rmsg.isFirstSent(rethrow().wrap(logger::getPreparedStatement))) {
                    logger.insertMessage(rmsg);
                    //Enter broadcast phase
                    broadcastToReplica(rmsg);
                } else {
                    //Relay to primary
                    super.send(addresses.get(primary), message);
                }
            } else if (message instanceof PreprepareMessage) {
                PreprepareMessage ppmsg = (PreprepareMessage) message;
                PublicKey publicKey = publicKeyMap.get(ppmsg.getClientInfo());

                if (ppmsg.isVerified(publicKey, this.primary, this::getWatermarks, rethrow().wrap(logger::getPreparedStatement))) {
                    logger.insertMessage(message);
                }
                broadcastPrepareMessage(ppmsg);
            } else if (message instanceof PrepareMessage) {
                PrepareMessage pmsg = (PrepareMessage) message;
                PublicKey publicKey = publicKeyMap.get(addresses.get(pmsg.getReplicaNum()));
                if (pmsg.isVerified(publicKey, this.primary, this::getWatermarks)) {
                    logger.insertMessage(pmsg);
                }
                if (pmsg.isPrepared(rethrow().wrap(logger::getPreparedStatement), getMaximumFaulty(), this.myNumber)) {
                    CommitMessage commitMessage = new CommitMessage(
                            this.getPrivateKey(),
                            pmsg.getViewNum(),
                            pmsg.getSeqNum(),
                            pmsg.getDigest(),
                            this.myNumber);
                    broadcastCommitMessage(commitMessage);
                }
            } else if (message instanceof CommitMessage) {
                CommitMessage cmsg = (CommitMessage) message;
                if (cmsg.isCommittedLocal(rethrow().wrap(logger::getPreparedStatement), getMaximumFaulty(), this.myNumber)) {
                    //TODO: Implement sequential execution
                    Operation operation = logger.getOperation(cmsg);
                    Result ret = operation.execute();
                    ReplyMessage replyMessage = new ReplyMessage(
                            getPrivateKey(),
                            cmsg.getViewNum(),
                            operation.getTimestamp(),
                            operation.getClientInfo(),
                            this.myNumber,
                            ret);

                    send(replyMessage.getClientInfo(), replyMessage);
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

    public void broadcastToReplica(RequestMessage message) {
        try {
            int seqNum = getLatestSequenceNumber() + 1;
            PreprepareMessage preprepareMessage = new PreprepareMessage(this.getPrivateKey(), this.primary, seqNum, message.getOperation());
            //Broadcast messages
            addresses.parallelStream().forEach(address -> send(address, preprepareMessage));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void broadcastPrepareMessage(PreprepareMessage message) {
        PrepareMessage prepareMessage = new PrepareMessage(
                this.getPrivateKey(),
                message.getViewNum(),
                message.getSeqNum(),
                message.getDigest(),
                this.myNumber);
        addresses.parallelStream().forEach(address -> send(address, prepareMessage));
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

    public void broadcastCommitMessage(Message message) {

        //TODO: Close connection
        //TODO: Delete client's public key
        //TODO: When sequence number == highWatermark, go to checkpoint phase and update a new lowWatermark
    }


}
