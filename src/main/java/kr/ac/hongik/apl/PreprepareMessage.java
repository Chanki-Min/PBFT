package kr.ac.hongik.apl;

import java.io.Serializable;
import java.security.*;

import static kr.ac.hongik.apl.Util.hash;
import static kr.ac.hongik.apl.Util.sign;

public class PreprepareMessage implements Message {
    private Operation operation;
    private Data data;
    private byte[] signature;

    /**
     * @param privateKey for digital signature
     * @param viewNum    current view number represents current leader.
     *                   Each replicas can access .properties file to get its own number.
     * @param seqNum     Current sequence number to identify. It didn't yet reach to agreement.
     * @param operation
     */
    PreprepareMessage(PrivateKey privateKey, final int viewNum, final int seqNum, final Operation operation) {
        this.operation = operation;
        try {
            this.data = new Data(viewNum, seqNum, operation);
            this.signature = sign(privateKey, this.data);
        } catch (NoSuchProviderException | InvalidKeyException | SignatureException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    int getViewNum() {
        return this.data.viewNum;
    }

    int getSeqNum() {
        return this.data.seqNum;
    }

    String getDigest() {
        return this.data.digest;
    }

    public byte[] getSignature() {
        return this.signature;
    }

    Operation getOperation() {
        return this.operation;
    }

    private class Data implements Serializable {
        private int viewNum;
        private int seqNum;
        private String digest;

        private Data(final int viewNum, final int seqNum, Operation operation) {
            this.viewNum = viewNum;
            this.seqNum = seqNum;
            this.digest = hash(operation);
        }
    }

}
