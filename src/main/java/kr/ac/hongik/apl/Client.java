package kr.ac.hongik.apl;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.Properties;

import static java.util.Base64.getEncoder;
import static kr.ac.hongik.apl.Util.serialize;

public class Client extends Connector {
    private static final String path = "src/main/resources/replica.properties";
    private Logger logger;


    public Client(Properties prop){
        super(prop);    //make socket to every replica
        this.logger = new Logger();
    }

    //Empty method.
    @Override
    @Deprecated
    protected void acceptOp(SelectionKey key) { }

    private void request(Message msg){

    }

    Result getReply() {
        ReplyMessage replyMessage;
        while (true) {
            replyMessage = (ReplyMessage) receive();
            // check client info
            //TODO: client - replica간 public 키 공유 구현하기
            PublicKey publicKey = this.publicKeyMap.get(this.addresses.get(replyMessage.getReplicaNum()));
            if (replyMessage.verifySignature(publicKey)) {
                logger.insertMessage(replyMessage);
                if (countSameMessages(replyMessage) > getMaximumFaulty()) {
                    //Reached an agreement
                    //Delete messages in DB
                    String query = "DELETE FROM Replies R WHERE R.timestamp = ? AND R.result = ?";
                    try (var pstmt = logger.getPreparedStatement(query)) {
                        pstmt.setLong(1, replyMessage.getTime());
                        pstmt.setString(2, getEncoder().encodeToString(serialize(replyMessage.getResult())));
                        pstmt.execute();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    break;
                }
            }
        }
        return replyMessage.getResult();
    }

    private int countSameMessages(ReplyMessage replyMessage) {
        String query = "SELECT COUNT(*) FROM Replies R WHERE R.client = ? AND R.timestamp = ? AND R.result = ?";

        try (var pstmt = logger.getPreparedStatement(query)) {
            pstmt.setString(1, getEncoder().encodeToString(serialize(replyMessage.getClientInfo().getAddress())));
            pstmt.setLong(2, replyMessage.getTime());
            pstmt.setString(3, getEncoder().encodeToString(serialize(replyMessage.getResult())));
            try (var ret = pstmt.executeQuery()) {
                ret.next();
                return ret.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return 1;
        }
    }

    private static Properties readProperties() throws IOException {
        Properties properties = new Properties();
        FileInputStream fis = new FileInputStream(path);
        properties.load(new java.io.BufferedInputStream(fis));

        return properties;
    }

    public static void main(String[] args) throws IOException {
        Client client = new Client(readProperties());
        client.connect();
    }
}
