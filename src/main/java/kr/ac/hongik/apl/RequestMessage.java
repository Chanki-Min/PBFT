package kr.ac.hongik.apl;


import java.net.InetSocketAddress;
import java.security.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.util.Base64.getEncoder;
import static kr.ac.hongik.apl.Util.serialize;
import static kr.ac.hongik.apl.Util.sign;

public class RequestMessage implements Message {

    private Operation operation;
    private byte[] signature;

    public RequestMessage(PrivateKey privateKey, Operation operation) {

        this.operation = operation;
        try {
            this.signature = sign(privateKey, this.operation);
        } catch (NoSuchProviderException | NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
            e.printStackTrace();
        }
    }

    boolean isFirstSent(Function<String, PreparedStatement> prepareStatement) {
        try {
            String baseQuery = "SELECT R.timestamp FROM Requests R WHERE R.client = ? AND R.operation = ?";
            var pstmt = prepareStatement.apply(baseQuery);
            pstmt.setString(1, getEncoder().encodeToString(serialize(this.getClientInfo())));
            pstmt.setString(2, getEncoder().encodeToString(serialize(this.getOperation())));
            var ret = pstmt.executeQuery();
            List<Long> timestamps = new ArrayList<>();
            while (ret.next()) {
                timestamps.add(ret.getLong(1));
            }
            return !timestamps.stream().anyMatch(prevTimestamp -> prevTimestamp < this.getTime());
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

    public InetSocketAddress getClientInfo() {
        return this.operation.getClientInfo();
    }

    public byte[] getSignature() {
        return this.signature;
    }
}
