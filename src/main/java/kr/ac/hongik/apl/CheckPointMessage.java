package kr.ac.hongik.apl;

import java.io.Serializable;
import java.security.*;

import static kr.ac.hongik.apl.Util.sign;

public class CheckPointMessage implements Serializable{
    private Data data;
    private  byte[] signature;

    public static CheckPointMessage makeCheckOutMessage(PrivateKey privateKey, int seqNum, String digest, int replicaNum) {
        Data data = new Data(seqNum, digest,replicaNum);
        byte[] signature = sign(privateKey, data);
        return new CheckPointMessage(data, signature);
    }

    private CheckPointMessage(Data data, byte[] signature) {
        this.data = data;
        this.signature = signature;
    }

    public boolean verify(PublicKey publicKey) {
        return Util.verify(publicKey, this.data, this.signature);
    }

    private static class Data implements Serializable {
        private final int seqNum;
        private final int replicaNum;
        private final String digest;

        private Data(int seqNum, String digest, int replicaNum) {
            this.seqNum = seqNum;
            this.digest = digest;
            this.replicaNum = replicaNum;
        }
    }
}