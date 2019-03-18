package kr.ac.hongik.apl;

import java.io.Serializable;
import java.security.PrivateKey;
import java.security.PublicKey;

import static kr.ac.hongik.apl.Util.sign;

class ReplyMessage implements Message {

    private final Data data;
    private byte[] signature;
    private final boolean isDistributed;

    public ReplyMessage(Data data, byte[] signature, boolean isDistributed) {
        this.data = data;
        this.signature = signature;
        this.isDistributed = isDistributed;
        if (Replica.DEBUG && isDistributed)
            System.err.println("Distributed request!");
    }

    public static ReplyMessage makeReplyMsg(PrivateKey privateKey, int viewNum, long timestamp,
                                            PublicKey clientInfo, int replicaNum, Object result, boolean isDistributed) {
        Data data = new Data(viewNum, timestamp, clientInfo, replicaNum, result);
        byte[] signature = sign(privateKey, data);

        return new ReplyMessage(data, signature, isDistributed);
    }

    public int getViewNum(){
        return this.data.viewNum;
    }

    public long getTime() {
        return this.data.time;
    }

    public PublicKey getClientInfo() {
        return this.data.clientInfo;
    }

    public int getReplicaNum(){
        return this.data.replicaNum;
    }

    public Object getResult() {
        return this.data.result;
    }

    public byte[] getSignature() {
        return this.signature;
    }

    public ReplyMessage setSignature(byte[] signature) {
        this.signature = signature;
        return this;
    }

    public Data getData() {
        return this.data;
    }


    public boolean verifySignature(PublicKey publicKey) {
        return Util.verify(publicKey, this.data, this.signature);
    }

    public boolean isDistributed() {
        return isDistributed;
    }

    private static class Data implements Serializable {
        private int viewNum;
        private long time;
        private PublicKey clientInfo;
        private int replicaNum;
        private Object result;

        private Data(int viewNum, long time, PublicKey clientInfo, int replicaNum, Object result) {
            this.viewNum = viewNum;
            this.time = time;
            this.clientInfo = clientInfo;
            this.replicaNum = replicaNum;
            this.result = result;
        }
    }
}

