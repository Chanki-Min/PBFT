package kr.ac.hongik.apl;

import java.io.Serializable;
import java.security.*;
import java.util.Arrays;
import java.util.function.Supplier;

import static kr.ac.hongik.apl.Util.sign;
import static kr.ac.hongik.apl.Util.verify;

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


    boolean isVerified(PublicKey publicKey, final int currentPrimary, Supplier<int[]> watermarkGetter) {
        Boolean[] checklist = new Boolean[3];

        checklist[0] = verify(publicKey, this.data, this.signature);

        checklist[1] = currentPrimary == this.getViewNum();

        int[] watermarks = watermarkGetter.get();
        int lowWatermark = watermarks[0],
                highWatermark = watermarks[1];
        checklist[2] = (lowWatermark <= this.getSeqNum()) && (this.getSeqNum() <= highWatermark);

        return Arrays.stream(checklist).allMatch(x -> x);
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
