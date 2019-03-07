package kr.ac.hongik.apl;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Properties;

public class Client extends Connector {
    private HashMap<Object, Integer> replies = new HashMap<>();


    public Client(Properties prop){
        super(prop);    //make socket to every replica
        super.connect();
    }

    @Override
    protected void sendHeaderMessage(SocketChannel channel) {
        HeaderMessage headerMessage = new HeaderMessage(-1, this.getPublicKey(), "client");
		send(channel, headerMessage);
    }

    //Empty method.
    @Override
    protected void acceptOp(SelectionKey key) {
        throw new UnsupportedOperationException("Client class does not need this method.");
    }

    public void request(RequestMessage msg) {
        if(Replica.DEBUG){
            System.err.println(" Time : " + msg.getTime());
        }
        this.replicas.values().forEach(channel -> this.send(channel, msg));
    }

    private void handleHeaderMessage(HeaderMessage message) {
        HeaderMessage header = message;
        if (!header.getType().equals("replica")) throw new AssertionError();
        SocketChannel channel = header.getChannel();
        this.publicKeyMap.put(channel, header.getPublicKey());
        this.replicas.put(header.getReplicaNum(), channel);
    }

    Result getReply() {
        ReplyMessage replyMessage;
        while (true) {
            Message message = receive();
            if(message instanceof HeaderMessage) {
                handleHeaderMessage((HeaderMessage) message);
                continue;
            }
            replyMessage = (ReplyMessage) message;
            // check client info
            PublicKey publicKey = this.publicKeyMap.get(this.replicas.get(replyMessage.getReplicaNum()));
            if (replyMessage.verifySignature(publicKey)) {
                /*
                 * 한 클라이언트가 여러 request를 보낼 시, 그 request들을 구분해주는 것은 timestamp이므로,
                 * Timestamp값을 이용하여 여러 요청들을 구분한다.
                 */
                var uniqueKey = replyMessage.getTime();
                Integer numOfValue = replies.getOrDefault(uniqueKey, 0);
				replies.put(uniqueKey, numOfValue + 1);
                if(replies.get(uniqueKey) > this.getMaximumFaulty()) {
                    //replies.remove(replyMessage);
                    if(Replica.DEBUG){
                        System.err.printf("consensus made! Time : %d ", replyMessage.getTime());
                    }
                    return replyMessage.getResult();
                }
            } else {
                System.err.println("Message not verified");
            }
        }
    }


    public static void main(String[] args) throws IOException {
    }
}
