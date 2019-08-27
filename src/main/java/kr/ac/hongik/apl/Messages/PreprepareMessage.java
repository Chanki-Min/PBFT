package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Replica;

import java.io.Serializable;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static kr.ac.hongik.apl.Util.*;

public class PreprepareMessage implements Message {
	private final RequestMessage requestMessage;
	private final Data data;
	private byte[] signature;

	private PreprepareMessage(Data data, byte[] signature, RequestMessage requestMessage) {
		this.data = data;
		this.signature = signature;
		this.requestMessage = requestMessage;
	}

	/**
	 * @param privateKey     for digital signature
	 * @param viewNum        current view number represents current leader.
	 *                       Each replicas can access .properties file to get its own number.
	 * @param seqNum         Current sequence number to identify. It didn't yet reach to agreement.
	 * @param requestMessage
	 */

	public static PreprepareMessage makePrePrepareMsg(PrivateKey privateKey, int viewNum, int seqNum, RequestMessage requestMessage) {
		Data data = new Data(viewNum, seqNum, requestMessage);
		byte[] sig = sign(privateKey, data);
		return new PreprepareMessage(data, sig, requestMessage);
	}

	/**
	 * Checks for signature, watermark, current view, and duplication
	 *
	 * @param primaryPublicKey
	 * @param currentView
	 * @param clientPublicKey
	 * @param prepareStatement
	 * @return
	 */

	public boolean isVerified(PublicKey primaryPublicKey,
							  final int currentView,
							  PublicKey clientPublicKey,
							  Function<String, PreparedStatement> prepareStatement) {
		Boolean[] checklist = new Boolean[4];

		checklist[0] = verify(primaryPublicKey, this.data, this.signature);

		checklist[1] = getViewNum() == currentView;

		checklist[2] = checkUniqueTuple(prepareStatement);

		checklist[3] = requestMessage.verify(clientPublicKey);
		if (Replica.DEBUG) {
			Arrays.stream(checklist).forEach(x -> System.err.print(" " + x + " "));
			System.err.println(" ");
		}
		return Arrays.stream(checklist).allMatch(x -> x);
	}

	public boolean equals(PrepareMessage obj) {
		return this.data.digest.equals(obj.getDigest()) &&
				this.data.seqNum == obj.getSeqNum() &&
				this.data.viewNum == obj.getViewNum();
	}

	public int getViewNum() {
		return this.data.viewNum;
	}

	private boolean checkUniqueTuple(Function<String, PreparedStatement> prepareStatement) {
		String baseQuery = "SELECT DISTINCT P.digest FROM Preprepares P WHERE P.viewNum = ? AND P.seqNum = ?";
		List<String> digests = new ArrayList<>();
		try (var pstmt = prepareStatement.apply(baseQuery)) {
			pstmt.setInt(1, getViewNum());
			pstmt.setInt(2, getSeqNum());
			try (var ret = pstmt.executeQuery()) {
				while (ret.next()) {
					digests.add(ret.getString(1));
				}
			}
			return digests.size() == 0 || digests.size() == 1;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	public int getSeqNum() {
		return this.data.seqNum;
	}

	Data getData() {
		return this.data;
	}

	public String getDigest() {
		return this.data.digest;
	}

	public byte[] getSignature() {
		return this.signature;
	}

	PreprepareMessage setSignature(byte[] signature) {
		this.signature = signature;
		return this;
	}

	PublicKey getClientInfo() {
		return this.getOperation().getClientInfo();
	}

	public Operation getOperation() {
		return this.requestMessage.getOperation();
	}

	public RequestMessage getRequestMessage() {
		return this.requestMessage;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PreprepareMessage)
			return hashCode() == obj.hashCode();
		else
			return super.equals(obj);

	}

	@Override
	public int hashCode() {
		return (getViewNum() + getSeqNum() + getDigest()).hashCode();

	}

	private static class Data implements Serializable {
		private int viewNum;
		private int seqNum;
		private String digest;

		private Data(final int viewNum, final int seqNum, RequestMessage requestMessage) {
			this.viewNum = viewNum;
			this.seqNum = seqNum;
			this.digest = hash(requestMessage);
		}
	}

}
