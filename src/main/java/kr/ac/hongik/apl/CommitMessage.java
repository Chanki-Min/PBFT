package kr.ac.hongik.apl;

import java.io.Serializable;
import java.security.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

import static kr.ac.hongik.apl.Util.sign;

public class CommitMessage implements Message {
    byte[] signature;
    private Data data;

    public CommitMessage(PrivateKey privateKey, int viewNum, int seqNum, String digest, int replicaNum) {
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

    boolean isCommittedLocal(Function<String, PreparedStatement> prepareStatement, int maxFaulty, int replicaNum) {
        Boolean[] checklist = new Boolean[2];
        //Predicate isPrepared(m, v, n, i) is true
        PrepareMessage prepareMessage = PrepareMessage.fromCommitMessage(this);
        checklist[0] = prepareMessage.isPrepared(prepareStatement, maxFaulty, replicaNum);
        //And got 2f+1 same commit messages
        String baseQuery = new StringBuilder()
                .append("SELECT DISTINCT count(*) ")
                .append("FROM Commits C")
                .append("WHERE C.seqNum = ? AND C.replica = ?")
                .toString();
        try (var pstmt = prepareStatement.apply(baseQuery)) {
            pstmt.setInt(1, this.getSeqNum());
            pstmt.setInt(2, replicaNum);

            try (var ret = pstmt.executeQuery()) {
                ret.next();
                checklist[1] = ret.getInt(1) > maxFaulty * 2;
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
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


