package kr.ac.hongik.apl;


import java.io.Serializable;
import java.net.InetSocketAddress;
import java.security.*;
import java.text.SimpleDateFormat;

import static kr.ac.hongik.apl.Util.sign;

public class RequestMessage implements Message {

    private Data data;
    private byte[] signature;

    public RequestMessage(PrivateKey privateKey, Operation operation, String ip, Integer port) {

        this.data = new Data(operation, ip, port);
        try {
            this.signature = sign(privateKey, this.data);
        } catch (NoSuchProviderException | NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            e.printStackTrace();
        }
    }

    public Operation getOperation() {
        return this.data.operation;
    }

    public long getTime() {
        return this.data.time;
    }

    public InetSocketAddress getClientInfo() {
        return this.data.clientInfo;
    }

    private class Data implements Serializable {
        private Operation operation;
        private long time;
        private InetSocketAddress clientInfo;

        public Data(Operation operation, String ip, Integer port) {
            this.operation = operation;
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.time = System.currentTimeMillis() / 1000;
            this.clientInfo = new InetSocketAddress(ip, port);
        }
    }

}
