package kr.ac.hongik.apl;

import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.function.Function;

public class Validation extends Operation {
	private final String header;
	private final String artHash;
	private Function<String, PreparedStatement> sqlAccessor = null;

	protected Validation(PublicKey clientInfo, String header, String artHash) {
		super(clientInfo, Instant.now().getEpochSecond());
		this.artHash = artHash;
		this.header = header;
	}

	private PreparedStatement getPreparedStatement(String query) {
		return sqlAccessor.apply(query);
	}

	@Override
	public Object execute() {
		var query = "SELECT B.header = ? " +
				"FROM Blocks B " +
				"WHERE B.txnTime = (SELECT MAX(B1.txnTime) " +
				"					FROM Blocks B1 " +
				"					WHERE B1.work = ?)";
		try (var pstmt = getPreparedStatement(query)) {
			pstmt.setString(1, header);
			pstmt.setString(2, artHash);
			try (var ret = pstmt.executeQuery()) {
				ret.next();
				return ret.getBoolean(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}


	public void setSqlAccessor(Function<String, PreparedStatement> sqlAccessor) {
		this.sqlAccessor = sqlAccessor;
	}

}
