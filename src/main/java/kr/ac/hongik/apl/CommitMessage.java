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
        this.signature = sign(privateKey, this.data);
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
                .append("SELECT DISTINCT count(C.replica) ")
                .append("FROM Commits C ")
                .append("WHERE C.seqNum = ?")
                .toString();
        try (var pstmt = prepareStatement.apply(baseQuery)) {
            pstmt.setInt(1, this.getSeqNum());

            try (var ret = pstmt.executeQuery()) {
                ret.next();
                var sameMessages = ret.getInt(1);
                if(Replica.DEBUG){
                    System.err.println("sameMessages : " + sameMessages);
                }
                checklist[1] = sameMessages > maxFaulty * 2;
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


