package kr.ac.hongik.apl;

import java.io.Serializable;
import java.security.*;

import static kr.ac.hongik.apl.Util.sign;

class ReplyMessage implements Message {

    private final Data data;
    private byte[] signature;

    public ReplyMessage(PrivateKey privateKey, int viewNum, long time, PublicKey clientInfo, int replicaNum, Result result) {

        this.data = new Data(viewNum, time, clientInfo, replicaNum, result);
        this.signature = sign(privateKey, this.data);
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

    public Result getResult() {
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

    private static class Data implements Serializable {
        private int viewNum;
        private long time;
        private PublicKey clientInfo;
        private int replicaNum;
        private Result result;

        private Data(int viewNum, long time, PublicKey clientInfo, int replicaNum, Result result) {
            this.viewNum = viewNum;
            this.time = time;
            this.clientInfo = clientInfo;
            this.replicaNum = replicaNum;
            this.result = result;
        }
    }
}

