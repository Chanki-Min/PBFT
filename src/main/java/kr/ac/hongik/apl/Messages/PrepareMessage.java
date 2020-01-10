package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;

import java.io.Serializable;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static kr.ac.hongik.apl.Util.sign;

public class PrepareMessage implements Message {
	byte[] signature;
	private Data data;

	private PrepareMessage(Data data, byte[] signature) {
		this.data = data;
		this.signature = signature;
	}

	private PrepareMessage() {
	}

	public static PrepareMessage makePrepareMsg(PrivateKey privateKey, int viewNum, int seqNum, String digest, int replicaNum) {
		Data data = new Data(viewNum, seqNum, digest, replicaNum);
		byte[] signature = sign(privateKey, data);
		return new PrepareMessage(data, signature);
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


	public boolean isVerified(PublicKey publicKey, final int currentView, Supplier<int[]> watermarkGetter) {
		Boolean[] checklist = new Boolean[3];

		checklist[0] = this.verify(publicKey);

		checklist[1] = currentView == this.getViewNum();

		int[] watermarks = watermarkGetter.get();
		int lowWatermark = watermarks[0],
				highWatermark = watermarks[1];

		checklist[2] = (lowWatermark <= this.getSeqNum()) && (this.getSeqNum() < highWatermark);

		Replica.detailDebugger.trace(String.format("verify result : %s ", Arrays.toString(checklist)));

		return Arrays.stream(checklist).allMatch(x -> x);
	}

	public boolean isPrepared(BiFunction<String, String, PreparedStatement> prepareStatement, int maxfaulty, int replicaNum) {

		Boolean[] checklist = new Boolean[3];
		String baseQuery;
		//Check for the pre-prepare message that matches to this prepare message
		baseQuery = new StringBuilder()
				.append("SELECT COUNT(*) ")
				.append("FROM Preprepares AS P ")
				.append("WHERE P.viewNum = ? AND P.seqNum = ? AND P.digest = ?")
				.toString();
		try (var pstmt = prepareStatement.apply(Logger.CONSENSUS,baseQuery)) {
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
		try (var pstmt = prepareStatement.apply(Logger.CONSENSUS, baseQuery)) {
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

		return Arrays.stream(checklist).allMatch(x -> x);
	}

	public boolean equals(PreprepareMessage pp) {
		return this.getDigest().equals(pp.getDigest()) &&
				this.getSeqNum() == pp.getSeqNum();
	}

	public boolean verify(PublicKey publicKey) {
		return Util.verify(publicKey, this.data, this.signature);
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
