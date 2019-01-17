package kr.ac.hongik.apl;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.security.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;

import static kr.ac.hongik.apl.Util.*;

public class PreprepareMessage implements Message {
    private Operation operation;
    private Data data;
    private byte[] signature;

    /**
     * @param privateKey for digital signature
     * @param viewNum    current view number represents current leader.
     *                   Each replicas can access .properties file to get its own number.
     * @param seqNum     Current sequence number to identify. It didn't yet reach to agreement.
     * @param operation
     */
    PreprepareMessage(PrivateKey privateKey, final int viewNum, final int seqNum, final Operation operation) {
        this.operation = operation;
        try {
            this.data = new Data(viewNum, seqNum, operation);
            this.signature = sign(privateKey, this.data);
        } catch (NoSuchProviderException | InvalidKeyException | SignatureException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private boolean doesNotExistInDB(Function<String, PreparedStatement> prepareStatement) {
        try {
            String baseQuery = "SELECT COUNT(*) FROM Preprepares P WHERE P.viewNum = ? AND P.seqNum = ?";
            var pstmt = prepareStatement.apply(baseQuery);
            pstmt.setInt(1, getViewNum());
            pstmt.setInt(2, getSeqNum());
            var ret = pstmt.executeQuery();
            ret.next();             //fetch the data
            return ret.getInt(1) == 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }


    /**
     * Checks for signature, watermark, current view, and duplication
     * @param publicKey
     * @param currentPrimary
     * @param watermarkGetter
     * @param prepareStatement
     * @return
     */
    boolean isVerified(PublicKey publicKey,
                       final int currentPrimary,
                       Supplier<int[]> watermarkGetter,
                       Function<String, PreparedStatement> prepareStatement) {
        Boolean[] checklist = new Boolean[4];

        checklist[0] = verify(publicKey, this.data, this.signature);

        checklist[1] = getViewNum() == currentPrimary;

        checklist[2] = doesNotExistInDB(prepareStatement);

        int[] watermarks = watermarkGetter.get();
        checklist[3] = (watermarks[0] <= getSeqNum()) && (getSeqNum() <= watermarks[1]);

        return Arrays.stream(checklist).allMatch(x -> x);
    }

    int getViewNum() {
        return this.data.viewNum;
    }

    int getSeqNum() {
        return this.data.seqNum;
    }

    String getDigest() {
        return this.data.digest;
    }

    public byte[] getSignature() {
        return this.signature;
    }

    Operation getOperation() {
        return this.operation;
    }

    InetSocketAddress getClientInfo() {
        return this.getOperation().getClientInfo();
    }

    private class Data implements Serializable {
        private int viewNum;
        private int seqNum;
        private String digest;

        private Data(final int viewNum, final int seqNum, Operation operation) {
            this.viewNum = viewNum;
            this.seqNum = seqNum;
            this.digest = hash(operation);
        }
    }

}
