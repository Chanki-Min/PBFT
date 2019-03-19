package kr.ac.hongik.apl;


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

    public RequestMessage(PrivateKey privateKey, Operation operation) {

        this.operation = operation;
        this.signature = sign(privateKey, this.operation);
    }

    private RequestMessage(Operation operation, byte[] signature) {
        this.operation = operation;
        this.signature = signature;
    }

    public static RequestMessage makeRequestMsg(PrivateKey privateKey, Operation operation) {
		byte[] signature;
		if (operation instanceof CertCreation)
			signature = sign(privateKey, operation.toString());
		else
			signature = sign(privateKey, operation);
        return new RequestMessage(operation, signature);
    }

    boolean verify(PublicKey publicKey) {
		if (this.operation instanceof CertCreation)
			return Util.verify(publicKey, this.operation.toString(), this.signature);
		else
			return Util.verify(publicKey, this.operation, this.signature);
    }

    boolean isFirstSent(Function<String, PreparedStatement> prepareStatement) {
        try {
            String baseQuery = "SELECT R.timestamp FROM Requests R WHERE R.client = ? AND R.operation = ?";
            var pstmt = prepareStatement.apply(baseQuery);
            pstmt.setString(1, getEncoder().encodeToString(serialize(this.getClientInfo())));
            pstmt.setString(2, getEncoder().encodeToString(serialize(this.getOperation())));
            var ret = pstmt.executeQuery();
            while(ret.next()){
                if(this.getTime() == ret.getLong(1))
                    return false;
            }
            return true;
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
