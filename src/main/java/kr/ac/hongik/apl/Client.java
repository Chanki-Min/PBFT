package kr.ac.hongik.apl;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Properties;

public class Client extends Connector {
    private HashMap<Message, Integer> hashMap = new HashMap<>();


    public Client(Properties prop){
        super(prop);    //make socket to every replica
        super.connect();
    }

    //Empty method.
    @Override
    protected void acceptOp(SelectionKey key) {
        throw new UnsupportedOperationException("Client class does not need this method.");
    }

    public void request(Message msg) {
        addresses.stream().peek(x -> {
            if (Replica.DEBUG) System.err.printf("client -> %s:%s\n", x.getAddress(), x.getPort());
        }).forEach(x -> this.send(x, msg));
    }

    Result getReply() {
        ReplyMessage replyMessage;
        while (true) {
            replyMessage = (ReplyMessage) receive();
            // check client info
            PublicKey publicKey = this.publicKeyMap.get(this.addresses.get(replyMessage.getReplicaNum()));
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
