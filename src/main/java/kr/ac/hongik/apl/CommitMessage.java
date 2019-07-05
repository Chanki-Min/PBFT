package kr.ac.hongik.apl;

import java.io.Serializable;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;

import static kr.ac.hongik.apl.Util.sign;

public class CommitMessage implements Message {
    byte[] signature;
    private Data data;


	private CommitMessage(Data data, byte[] signature) {
		this.data = data;
		this.signature = signature;
	}

	public static CommitMessage makeCommitMsg(PrivateKey privateKey, int viewNum, int seqNum, String digest, int replicaNum) {
		Data data = new Data(viewNum, seqNum, digest, replicaNum);
		byte[] signature = sign(privateKey, data);
		return new CommitMessage(data, signature);
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

    public boolean verify(PublicKey key) {
        return Util.verify(key, this.data, this.signature);
    }

    boolean isCommittedLocal(Function<String, PreparedStatement> prepareStatement, int maxFaulty, int replicaNum) {
        Boolean[] checklist = new Boolean[2];
        //Predicate isPrepared(m, v, n, i) is true
        String query = "SELECT count(*) FROM Commits WHERE seqNum =? AND replica= ?";
        try (var pstmt = prepareStatement.apply(query)) {
            pstmt.setInt(1, this.getSeqNum());
            pstmt.setInt(2, replicaNum);
            try (var ret = pstmt.executeQuery()) {
                ret.next();
                checklist[0] = (ret.getInt(1) == 1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        //And got 2f+1 same commit messages
        String baseQuery = new StringBuilder()
                .append("SELECT DISTINCT count(C.replica) ")
                .append("FROM Commits C ")
                .append("WHERE C.seqNum = ? AND C.digest = ?")
                .toString();
        try (var pstmt = prepareStatement.apply(baseQuery)) {
            pstmt.setInt(1, this.getSeqNum());
            pstmt.setString(2, this.getDigest());
            try (var ret = pstmt.executeQuery()) {
                ret.next();
                var sameMessages = ret.getInt(1);
                checklist[1] = sameMessages == maxFaulty * 2 + 1;
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
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


