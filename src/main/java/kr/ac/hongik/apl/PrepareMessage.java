package kr.ac.hongik.apl;

import java.io.Serializable;
import java.security.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static kr.ac.hongik.apl.Util.sign;
import static kr.ac.hongik.apl.Util.verify;

public class PrepareMessage implements Message {
    byte[] signature;
    private Data data;

    private PrepareMessage() {
    }

    public PrepareMessage(PrivateKey privateKey, int viewNum, int seqNum, String digest, int replicaNum) {
        this.data = new Data(viewNum, seqNum, digest, replicaNum);
        this.signature = sign(privateKey, this.data);
    }

    static public PrepareMessage fromCommitMessage(CommitMessage commitMessage) {
        PrepareMessage prepareMessage = new PrepareMessage();
        Data data = new Data(commitMessage.getViewNum(), commitMessage.getSeqNum(), commitMessage.getDigest(), commitMessage.getReplicaNum());
        prepareMessage.data = data;
        prepareMessage.signature = commitMessage.getSignature();

        return prepareMessage;
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

    boolean isPrepared(Function<String, PreparedStatement> prepareStatement, int maxfaulty, int replicaNum) {

        Boolean[] checklist = new Boolean[3];
        String baseQuery;
        //Check for the pre-prepare message that matches to this prepare message
        baseQuery = new StringBuilder()
                .append("SELECT COUNT(*) ")
                .append("FROM Preprepares AS P ")
                .append("WHERE P.viewNum = ? AND P.seqNum = ? AND P.digest = ?")
                .toString();
        try (var pstmt = prepareStatement.apply(baseQuery)) {
            pstmt.setInt(1, this.getViewNum());
            pstmt.setInt(2, this.getSeqNum());
            pstmt.setString(3, this.getDigest());
            try (var ret = pstmt.executeQuery()) {
                ret.next();
                checklist[0] = ret.getInt(1) == 1;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        //Checks for 2f+1 prepare messages that match (m, v, n) to this message
        baseQuery = new StringBuilder()
                .append("SELECT P.replica ")
                .append("FROM Prepares P ")
                .append("WHERE P.digest = ? AND P.viewNum = ? AND P.seqNum = ?")
                .toString();
        List<Integer> matchedPrepareMessages = new ArrayList<>();
        try (var pstmt = prepareStatement.apply(baseQuery)) {
            pstmt.setString(1, this.getDigest());
            pstmt.setInt(2, this.getViewNum());
            pstmt.setInt(3, this.getSeqNum());

            int sameMessages;
            try (var ret = pstmt.executeQuery()) {
                while (ret.next())
                    matchedPrepareMessages.add(ret.getInt(1));
            }
            checklist[1] = matchedPrepareMessages.size() > maxfaulty * 2;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

        //Checks for i'th replica verify its prepare message
        checklist[2] = matchedPrepareMessages.stream().anyMatch(x -> x == replicaNum);
        if(Replica.DEBUG){
            System.err.println(checklist[0] + " " + checklist[1] + " " + checklist[2]);
        }

        return Arrays.stream(checklist).allMatch(x -> x);
    }

    private static class Data implements Serializable {
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
