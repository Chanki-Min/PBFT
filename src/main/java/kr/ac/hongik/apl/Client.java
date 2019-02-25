package kr.ac.hongik.apl;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Properties;

public class Client extends Connector {
    private HashMap<Message, Integer> hashMap = new HashMap<>();


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
            PublicKey publicKey = this.publicKeyMap.get(this.replicaAddresses.get(replyMessage.getReplicaNum()));
            if (replyMessage.verifySignature(publicKey)) {
                Integer numOfValue = null;
                if((numOfValue = hashMap.get(replyMessage)) == null){
                    hashMap.put(replyMessage, 1);
                }
                else if(numOfValue < numOfReplica - 1){
                    hashMap.put(replyMessage, ++numOfValue);
                }
                else{
                    hashMap.remove(replyMessage);
                    return replyMessage.getResult();
                }
            }
        }
    }


    public static void main(String[] args) throws IOException {
    }
}
