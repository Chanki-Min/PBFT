package kr.ac.hongik.apl;

import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

public class Validation extends Operation {
	private final String header;
	private final String artHash;
	private Function<String, PreparedStatement> sqlAccessor = null;

	protected Validation(PublicKey clientInfo, String header, String artHash) {
		super(clientInfo);
		this.artHash = artHash;
		this.header = header;
	}

	private PreparedStatement getPreparedStatement(String query) {
		return sqlAccessor.apply(query);
	}

	@Override
	public Object execute(Logger logger) {
		var query = "SELECT B.header " +
				"FROM Blocks B " +
				"WHERE B.txnTime = (SELECT MAX(B1.txnTime) " +
				"					FROM Blocks B1 " +
				"					WHERE B1.work = ?)";
		try (var pstmt = getPreparedStatement(query)) {
			//pstmt.setString(1, header);
			pstmt.setString(1, artHash);
			try (var ret = pstmt.executeQuery()) {
				ret.next();
				var hdr = ret.getString(1);
				return header.equals(hdr);
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
