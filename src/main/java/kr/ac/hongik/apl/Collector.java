package kr.ac.hongik.apl;


import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

public class Collector extends Operation {
	private final String root;
	private Function<String, PreparedStatement> sqlAccessor = null;
	private Integer replicaNumber = null;

	public Collector(PublicKey clientInfo, String root) {
		super(clientInfo, true);
		this.root = root;
	}


	@Override
	public Object execute(Logger logger) {
		String query = "SELECT C.piece FROM Certs C WHERE C.root = ?";
		try (var pstmt = getPreparedStatement(query)) {
			pstmt.setString(1, root);
			try (var ret = pstmt.executeQuery()) {
				if (ret.next()) {
					byte[] piece = ret.getBytes(1);
					return new Object[]{this.replicaNumber + 1, piece};
				} else
					throw new RuntimeException("Why not exist :(");
			}
		} catch (SQLException e) {
			System.err.println(e);
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
