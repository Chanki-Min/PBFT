package kr.ac.hongik.apl;

import java.nio.channels.SocketChannel;
import java.security.PublicKey;

public class HeaderMessage implements Message{
    private final int replicaNum;
    private final PublicKey publicKey;
    private final String type;
    private SocketChannel channel;

    public HeaderMessage(int replicaNum, PublicKey publicKey, String type) {
        this.replicaNum = replicaNum;
        this.publicKey = publicKey;
        this.type = type.toLowerCase();
    }

    public void setChannel(SocketChannel channel) {
        this.channel = channel;
    }

    public SocketChannel getChannel() {
        return this.channel;
    }

    public int getReplicaNum(){
        return this.replicaNum;
    }

    public PublicKey getPublicKey(){
        return this.publicKey;
    }

    public String getType(){
        return this.type;
    }
}
