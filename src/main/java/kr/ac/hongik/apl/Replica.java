package kr.ac.hongik.apl;

import java.io.IOException;
import java.io.InputStream;
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
import java.util.function.Supplier;

import static com.diffplug.common.base.Errors.rethrow;

public class Replica extends Connector {
    final static int WATERMARK_UNIT = 100;
    private final static double timeout = 1.;


    private final int myNumber;
    private int primary = 0;
    private ServerSocketChannel listener;
    private List<SocketChannel> clients;
    private InetSocketAddress myAddress;

    private Logger logger;
    private int lowWatermark;


    Replica(Properties prop, String serverIp, int serverPort) {
        super(prop);

        String loggerFileName = String.format("consensus_%s_%d.db", serverIp, serverPort);

        this.logger = new Logger(loggerFileName);
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

                    this.sendPublicKey(channel);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        acceptanceThread.setDaemon(true);
        acceptanceThread.start();

        super.connect();
    }

    public static void main(String[] args) throws IOException {
        String path = Replica.class.getResource("/replica.properties").getPath();
        try {
            String ip = args[0];
            int port = Integer.parseInt(args[1]);
            Properties properties = new Properties();
            InputStream is = Replica.class.getResourceAsStream("/replica.properties");
            properties.load(new java.io.BufferedInputStream(is));

            Replica replica = new Replica(properties, ip, port);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            replica.start();
        } catch (ArrayIndexOutOfBoundsException e) {
            System.err.println("Usage: program <ip> <port>");
        }
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

    void start() {
        //Assume that every connection is established
        while (true) {
            Message message = super.receive();              //Blocking method
            if (message instanceof RequestMessage) {
                handleRequestMessage((RequestMessage) message);
            } else if (message instanceof PreprepareMessage) {
                handlePreprepareMessage((PreprepareMessage) message);
            } else if (message instanceof PrepareMessage) {
                handlePrepareMessage((PrepareMessage) message);
            } else if (message instanceof CommitMessage) {
                handleCommitMessage((CommitMessage) message);
            } else
                throw new UnsupportedOperationException("Invalid message");
        }
    }

    private void handleCommitMessage(CommitMessage cmsg) {
        Supplier<Boolean> isCommitted = () -> cmsg.isCommittedLocal(rethrow().wrap(logger::getPreparedStatement),
                getMaximumFaulty(), this.myNumber);

        if (isCommitted.get()) {
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

            InetSocketAddress destination = publicKeyMap.entrySet().stream()
                    .filter(x -> x.getValue().equals(replyMessage.getClientInfo()))
                    .findFirst().get().getKey();
            send(destination, replyMessage);

            //TODO: Close connection
            //TODO: Delete client's public key
            //TODO: When sequence number == highWatermark, go to checkpoint phase and update a new lowWatermark
        }
    }

    private void handlePrepareMessage(PrepareMessage message) {
        PublicKey publicKey = publicKeyMap.get(addresses.get(message.getReplicaNum()));
        if (message.isVerified(publicKey, this.primary, this::getWatermarks)) {
            logger.insertMessage(message);
        }
        if (message.isPrepared(rethrow().wrap(logger::getPreparedStatement), getMaximumFaulty(), this.myNumber)) {
            CommitMessage commitMessage = new CommitMessage(
                    this.getPrivateKey(),
                    message.getViewNum(),
                    message.getSeqNum(),
                    message.getDigest(),
                    this.myNumber);
            addresses.forEach(address -> send(address, commitMessage));
        }
    }

    private void handleRequestMessage(RequestMessage message) {
        if (this.primary == this.myNumber
                && message.isFirstSent(rethrow().wrap(logger::getPreparedStatement))) {
            logger.insertMessage(message);
            //Enter broadcast phase
            broadcastToReplica(message);
        } else {
            //Relay to primary
            super.send(addresses.get(primary), message);
        }
    }

    private void handlePreprepareMessage(PreprepareMessage message) {
        PublicKey publicKey = publicKeyMap.get(message.getClientInfo());

        if (message.isVerified(publicKey, this.primary, this::getWatermarks, rethrow().wrap(logger::getPreparedStatement))) {
            logger.insertMessage(message);
        }
        PrepareMessage prepareMessage = new PrepareMessage(
                this.getPrivateKey(),
                message.getViewNum(),
                message.getSeqNum(),
                message.getDigest(),
                this.myNumber);
        addresses.forEach(address -> send(address, prepareMessage));
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

    private void broadcastToReplica(RequestMessage message) {
        try {
            int seqNum = getLatestSequenceNumber() + 1;
            PreprepareMessage preprepareMessage = new PreprepareMessage(this.getPrivateKey(), this.primary, seqNum, message.getOperation());
            logger.insertMessage(preprepareMessage);
            //Broadcast messages
            addresses.parallelStream().forEach(address -> send(address, preprepareMessage));
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
}
