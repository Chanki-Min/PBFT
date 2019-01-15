package kr.ac.hongik.apl;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.security.*;

import static kr.ac.hongik.apl.Util.sign;

class ReplyMessage implements Message {

    private Data data;
    private byte[] signature;

    public ReplyMessage(PrivateKey privateKey, int viewNum, long time, String ip, Integer port, int replicaNum, Result result) {

        this.data = new Data(viewNum, time, ip, port, replicaNum, result);
        try {
            this.signature = sign(privateKey, this.data);
        } catch (NoSuchProviderException | NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            e.printStackTrace();
        }
    }

    public int getViewNum(){
        return this.data.viewNum;
    }

    public long getTime() {
        return this.data.time;
    }

    public InetSocketAddress getClientInfo() {
        return this.data.clientInfo;
    }

    public int getReplicaNum(){
        return this.data.replicaNum;
    }

    public Result getResult() {
        return this.data.result;
    }

    private class Data implements Serializable {
        private int viewNum;
        private long time;
        private InetSocketAddress clientInfo;
        private int replicaNum;
        private Result result;

        public Data(int viewNum, long time, String ip, Integer port, int replicaNum, Result result) {
            this.viewNum = viewNum;
            this.time = time;
            this.clientInfo = new InetSocketAddress(ip, port);
            this.replicaNum = replicaNum;
            this.result = result;
        }
    }
}

