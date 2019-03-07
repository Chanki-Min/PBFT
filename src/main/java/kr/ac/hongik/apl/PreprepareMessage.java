package kr.ac.hongik.apl;

import java.io.Serializable;
import java.nio.channels.SocketChannel;
import java.security.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static kr.ac.hongik.apl.Util.*;

public class PreprepareMessage implements Message {
    private final Operation operation;
    private final Data data;
    private byte[] signature;

    public static PreprepareMessage makePrePrepareMsg(PrivateKey privateKey, int viewNum, int seqNum, Operation operation) {
        Data data = new Data(viewNum, seqNum, operation);
        var sig = sign(privateKey, data);
        return new PreprepareMessage(data, sig, operation);
    }

    /**
     * @param privateKey for digital signature
     * @param viewNum    current view number represents current leader.
     *                   Each replicas can access .properties file to get its own number.
     * @param seqNum     Current sequence number to identify. It didn't yet reach to agreement.
     * @param operation
     */
    PreprepareMessage(final PrivateKey privateKey, final int viewNum, final int seqNum, final Operation operation) {
        this.operation = operation;
        this.data = new Data(viewNum, seqNum, this.operation);
        this.signature = sign(privateKey, this.getData());
    }

    PreprepareMessage(Data data, byte[] signature, Operation operation) {
        this.data = data;
        this.signature = signature;
        this.operation = operation;
    }

    private boolean checkUniqueTuple(Function<String, PreparedStatement> prepareStatement) {
        String baseQuery = "SELECT DISTINCT P.digest FROM Preprepares P WHERE P.viewNum = ? AND P.seqNum = ?";
        List<String> digests = new ArrayList<>();
        try( var pstmt = prepareStatement.apply(baseQuery) ) {
            pstmt.setInt(1, getViewNum());
            pstmt.setInt(2, getSeqNum());
            try(var ret = pstmt.executeQuery()) {
                while(ret.next()) {
                    digests.add(ret.getString(1));
                }
            }
            return digests.size() == 0 || digests.size() == 1;
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

        //TODO: 항상 false 뜸 ㅠ

        checklist[0] = verify(publicKey, this.data, this.signature);

        checklist[1] = getViewNum() == currentPrimary;

        checklist[2] = checkUniqueTuple(prepareStatement);

        int[] watermarks = watermarkGetter.get();
        checklist[3] = (watermarks[0] <= getSeqNum()) && (getSeqNum() <= watermarks[1]);

        if(Replica.DEBUG){
            Arrays.stream(checklist).forEach(x -> System.err.print(x + " "));
            System.err.println();
        }


        return Arrays.stream(checklist).allMatch(x -> x);
    }

    Data getData(){ return this.data;}


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

    PreprepareMessage setSignature(byte[] signature) {
        this.signature = signature;
        return this;
    }

    Operation getOperation() {
        return this.operation;
    }

    PublicKey getClientInfo() {
        return this.getOperation().getClientInfo();
    }

    private static class Data implements Serializable {
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
