package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;
import org.echocat.jsu.JdbcUtils;

import java.io.Serializable;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.diffplug.common.base.Errors.rethrow;
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

	public boolean isCommittedLocal(BiFunction<String, String, PreparedStatement> prepareStatement, int maxFaulty, int replicaNum) {
        Boolean[] checklist = new Boolean[3];
        //Predicate isPrepared(m, v, n, i) is true
        String query = "SELECT count(*) FROM Commits WHERE seqNum = ? AND replica = ? AND digest = ?";
        try (var pstmt = prepareStatement.apply(Logger.CONSENSUS, query)) {
            pstmt.setInt(1, this.getSeqNum());
            pstmt.setInt(2, replicaNum);
            pstmt.setString(3, this.getDigest());
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
        try (var pstmt = prepareStatement.apply(Logger.CONSENSUS, baseQuery)) {
            pstmt.setInt(1, this.getSeqNum());
            pstmt.setString(2, this.getDigest());
            try (var ret = pstmt.executeQuery()) {
                ret.next();
                var sameMessages = ret.getInt(1);
                checklist[1] = sameMessages > maxFaulty * 2;
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        checklist[2] = !isAlreadyExecuted(prepareStatement, this.getSeqNum());
        Replica.detailDebugger.trace(String.format("verify result : %s ", Arrays.toString(checklist)));

        return Arrays.stream(checklist).allMatch(x -> x);
    }

    boolean isAlreadyExecuted(BiFunction<String, String, PreparedStatement> prepareStatement, int sequenceNumber) {
        String query = "SELECT seqNum FROM Executed E";
        try (var pstmt = prepareStatement.apply(Logger.CONSENSUS, query)) {
            try (var ret = pstmt.executeQuery()) {
                List<Integer> seqList = JdbcUtils.toStream(ret)
                        .map(rethrow().wrapFunction(x -> x.getInt(1)))
                        .collect(Collectors.toList());
                if(seqList.isEmpty())
                    return false;
                else {
                    return seqList.stream().max(Integer::compareTo).get() >= sequenceNumber;
                }
            }
        } catch (SQLException e) {
            Replica.msgDebugger.error(e);
            throw new RuntimeException(e);
        }
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


