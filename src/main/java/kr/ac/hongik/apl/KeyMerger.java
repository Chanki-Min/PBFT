package kr.ac.hongik.apl;


import java.net.InetSocketAddress;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public class KeyMerger extends Operation {
	private final List<InetSocketAddress> replicaAddresses;
	private final String root;
	private final String artHash;
	private int pieceNumber;
	private byte[] piece;
	private Function<String, PreparedStatement> sqlAccessor = null;
	private Integer replicaNumber = null;

	public KeyMerger(PublicKey clientInfo, Properties prop, String root, int pieceNumber, String artHash, byte[] piece) {
		super(clientInfo, Instant.now().getEpochSecond());
		replicaAddresses = Util.parseProperties(prop);
		this.root = root;
		this.pieceNumber = pieceNumber;
		this.artHash = artHash;
		this.piece = piece;
	}


	@Override
	public Object execute() {
		String query = "SELECT C.piece FROM Certs C WHERE C.root = ?";
		try (var pstmt = getPreparedStatement(query)) {
			pstmt.setString(1, root);
			try (var ret = pstmt.executeQuery()) {
				ret.next();
				byte[] piece = ret.getBytes(1);
				return new Object[]{this.replicaNumber + 1, piece};
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
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
