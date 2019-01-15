package kr.ac.hongik.apl;

import java.io.Serializable;
import java.security.*;

import static kr.ac.hongik.apl.Util.sign;

public class PrepareMessage implements Message {
    byte[] signature;
    private Data data;

    public PrepareMessage(PrivateKey privateKey, int viewNum, int seqNum, String digest, int replicaNum) {
        this.data = new Data(viewNum, seqNum, digest, replicaNum);
        try {
            this.signature = sign(privateKey, this.data);
        } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException | NoSuchProviderException e) {
            e.printStackTrace();
        }
    }

    public byte[] getSignature() {
        return this.signature;
    }

    public int getViewNum() {
        return this.data.viewNum;
    }

    public int getSeqNum() {
        return this.data.seqNum;
    }

    public String getDigest() {
        return this.data.digest;
    }

    public int getReplicaNum() {
        return this.data.replicaNum;
    }


    private class Data implements Serializable {
        private final int viewNum;
        private final int seqNum;
        private final String digest;
        private final int replicaNum;

        private Data(int viewNum, int seqNum, String digest, int replicaNum) {
            this.viewNum = viewNum;
            this.seqNum = seqNum;
            this.digest = digest;
            this.replicaNum = replicaNum;
        }
    }
}
