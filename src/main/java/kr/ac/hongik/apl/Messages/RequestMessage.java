package kr.ac.hongik.apl.Messages;


import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Util;

import java.security.PrivateKey;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

import static java.util.Base64.getEncoder;
import static kr.ac.hongik.apl.Util.serialize;
import static kr.ac.hongik.apl.Util.sign;

public class RequestMessage implements Message {

    private Operation operation;
    private byte[] signature;

    public static RequestMessage makeRequestMsg(PrivateKey privateKey, Operation operation) {
		byte[] signature;
		signature = sign(privateKey, operation);
        return new RequestMessage(operation, signature);
    }

    private RequestMessage(Operation operation, byte[] signature) {
        this.operation = operation;
        this.signature = signature;
    }


	public boolean verify(PublicKey publicKey) {
		return Util.verify(publicKey, this.operation, this.signature);
    }

	public boolean isNotRepeated(Function<String, PreparedStatement> prepareStatement) {
		//Client의 request timestamp는 항상 증가하는 방향으로만 이루어져야 한다.
		String baseQuery = "SELECT R.timestamp FROM Requests R WHERE R.client = ? ORDER BY R.timestamp DESC";
		try (var pstmt = prepareStatement.apply(baseQuery)) {
            pstmt.setString(1, getEncoder().encodeToString(serialize(this.getClientInfo())));
            var ret = pstmt.executeQuery();

			// 클라이언트의 첫 request이거나 가장 최신의 timestamp일 때만 true 반환
			return !ret.next() || this.getTime() > ret.getLong(1);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public Operation getOperation() {
        return this.operation;
    }

    public long getTime() {
        return this.operation.getTimestamp();
    }

    public PublicKey getClientInfo() {
        return this.operation.getClientInfo();
    }

    public byte[] getSignature() {
        return this.signature;
    }
}
