package kr.ac.hongik.apl;

import com.codahale.shamir.Scheme;

import java.net.InetSocketAddress;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/* TODO 구매 시나리오를 따라서 구현할 예정 */
public class Management extends Operation {
    private final List<InetSocketAddress> replicaAddresses;
    private final BlockPayload blockPayload;
    private Function<String, PreparedStatement> sqlAccessor = null;
    private Integer replicaNumber = null;

    protected Management(PublicKey clientInfo, Properties replicasInfo, BlockPayload blockPayload) {
        super(clientInfo, Instant.now().getEpochSecond());
        replicaAddresses = Util.parseProperties(replicasInfo);
        this.blockPayload = blockPayload;
    }

    private PreparedStatement getPreparedStatement(String query) throws SQLException {
        return sqlAccessor.apply(query);
    }

    private void createTableIfNotExists() {
        String query = "CREATE TABLE IF NOT EXISTS Blocks" +
                "(header TEXT, " +
                "work TEXT NOT NULL, " +
                "seller TEXT NOT NULL, " +
                "buyer TEXT NOT NULL, " +
                "price INT NOT NULL, " +
                "txnTime INT NOT NULL, " +
                "duration INT, " +
                "PRIMARY KEY(header))";
        try (var pstmt = getPreparedStatement(query)) {
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String getLatestheader() {
        String query = "SELECT B.header " +
                "FROM Blocks " +
                "WHERE B.txnTime = (SELECT MAX(B1.txnTime " +
                "                   FROM Blocks B1 )";
        try (var pstmt = getPreparedStatement(query)) {
            try (var ret = pstmt.executeQuery()) {
                ret.next();
                return ret.getString(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertBlock(String header, BlockPayload payload) {
        //TODO: fill
    }

    private void insertCert(final String root, final byte[] certPiece) {
        String query = "CREATE TABLE IF NOT EXISTS Certs " +
                "(root TEXT," +
                "piece TEXT," +
                "PRIMARY KEY(root)";
        try (var pstmt = getPreparedStatement(query)) {
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        query = "INSERT INTO Certs VALUES (?, ?)";
        try (var pstmt = getPreparedStatement(query)) {
            pstmt.setString(1, root);
            pstmt.setBytes(1, certPiece);

            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    /**
     * @return (cert piece, cert piece, merkle tree root)
     */
    @Override
    Object execute() {

        createTableIfNotExists();

        String prevHash = getLatestheader();

        String header = Util.hash(prevHash + blockPayload);

        insertBlock(header, blockPayload);

        //2. Split the certs and the replica store each pieces.

        int n = replicaAddresses.size() + 2;    //Replicas + seller + buyer

        //Assume that no one lost the piece
        final Scheme scheme = new Scheme(new SecureRandom(), n, n - 1);
        final Map<Integer, byte[]> pieces = scheme.split(header.getBytes());

        HashTree hashTree = new HashTree(pieces.values().stream().collect(Collectors.toList()));
        String root = hashTree.root.getHash();

        byte[] myPiece = pieces.get(replicaNumber);

        insertCert(root, myPiece);

        Object[] residual = pieces.values().stream().skip(replicaAddresses.size()).toArray();
        Object[] ret = {residual[0], residual[1], root};
        //TODO: Change the return type to Object if possible
        return null;
    }

    public boolean validate(String payloadHash, byte[] CertPiece) {
        return false;
    }

    /**
     * @param sqlAccessor If the replica identify that this message is Management instance,
     *                    then the replica insert the sql preparedStatement function.
     */
    public void setSqlAccessor(Function<String, PreparedStatement> sqlAccessor) {
        this.sqlAccessor = sqlAccessor;
    }


    public void setReplicaNumber(Integer replicaNumber) {
        this.replicaNumber = replicaNumber;
    }
}
