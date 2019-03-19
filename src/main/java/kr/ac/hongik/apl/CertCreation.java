package kr.ac.hongik.apl;

import java.net.InetSocketAddress;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CertCreation extends Operation {
	private final Map<Integer, byte[]> pieces;
	private final List<InetSocketAddress> replicaAddresses;
	private Function<String, PreparedStatement> sqlAccessor = null;
	private Integer replicaNumber = null;

	protected CertCreation(PublicKey clientInfo, Properties replicasInfo, Map<Integer, byte[]> pieces) {
		super(clientInfo, Instant.now().getEpochSecond(), true);
		replicaAddresses = Util.parseProperties(replicasInfo);
		this.pieces = pieces;
	}

	@Override
	public Object execute() {
		if (Replica.DEBUG) {
			System.err.println("Cert: execution");
		}
		byte[][] input = new byte[pieces.size()][];
		for (int i = 0; i < pieces.size(); ++i) {
			input[i] = pieces.get(i + 1);
		}

		HashTree hashTree = new HashTree(pieces.values().stream().collect(Collectors.toList()));
		String root = hashTree.root.getHash();
		byte[] myPiece = pieces.get(replicaNumber + 1);

		insertCert(root, myPiece);

		return root;
	}

	private void insertCert(final String root, final byte[] certPiece) {
		String query = "CREATE TABLE IF NOT EXISTS Certs " +
				"(root TEXT," +
				"piece TEXT," +
				"PRIMARY KEY(root))";
		try (var pstmt = getPreparedStatement(query)) {
			pstmt.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		query = "INSERT INTO Certs VALUES (?, ?)";
		try (var pstmt = getPreparedStatement(query)) {
			pstmt.setString(1, root);
			pstmt.setBytes(2, certPiece);

			pstmt.execute();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private PreparedStatement getPreparedStatement(String query) {
		return sqlAccessor.apply(query);
	}

	public void setSqlAccessor(Function<String, PreparedStatement> sqlAccessor) {
		this.sqlAccessor = sqlAccessor;
	}

	public void setReplicaNumber(Integer replicaNumber) {
		this.replicaNumber = replicaNumber;
	}

}
