package kr.ac.hongik.apl;

import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CertStorage extends Operation {
	private final Map<Integer, byte[]> pieces;
	private Function<String, PreparedStatement> sqlAccessor = null;
	private Integer replicaNumber = null;

	protected CertStorage(PublicKey clientInfo, Map<Integer, byte[]> pieces) {
		super(clientInfo, true);
		this.pieces = pieces;
	}

	@Override
	public String toString() {
		String ret = "";
		for (var k : pieces.keySet())
			ret += k.toString();
		for (var v : pieces.values())
			ret += new String(v);
		return ret;
	}

	@Override
	public Object execute() {
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
