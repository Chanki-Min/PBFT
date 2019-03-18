package kr.ac.hongik.apl;

import com.codahale.shamir.Scheme;

import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

public class Validation extends Operation {
	private final String cert;
	private final String artHash;
	private Function<String, PreparedStatement> sqlAccessor = null;

	protected Validation(PublicKey clientInfo, Properties prop, String root, Map<Integer, byte[]> pieces, String artHash) {
		super(clientInfo, Instant.now().getEpochSecond());
		this.artHash = artHash;
		this.cert = reassemble(pieces, root);
	}

	private String reassemble(Map<Integer, byte[]> pieces, String root) {
		String query = "SELECT C.scheme " +
				"FROM Certs C " +
				"WHERE C.root = ?";
		Scheme scheme;
		try (var pstmt = getPreparedStatement(query)) {
			pstmt.setString(1, root);
			try (var ret = pstmt.executeQuery()) {
				ret.next();
				scheme = (Scheme) ret.getObject(1);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		byte[] rootInBytes = scheme.join(pieces);

		return new String(rootInBytes);
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
			pstmt.setString(1, cert);
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
