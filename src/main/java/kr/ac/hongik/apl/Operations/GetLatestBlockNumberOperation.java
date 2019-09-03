package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.Logger;

import java.security.PublicKey;
import java.sql.SQLException;

public class GetLatestBlockNumberOperation extends Operation {
	public GetLatestBlockNumberOperation(PublicKey clientInfo) {
		super(clientInfo);
	}

	@Override
	public Object execute(Object obj) {
		Logger logger = (Logger) obj;
		try {
			String query = "SELECT max(idx) from BlockChain";
			var psmt = logger.getPreparedStatement(query);
			var rs = psmt.executeQuery();
			if (rs.next()) {
				return rs.getInt(1);
			}
		} catch (SQLException ignore) {
		}
		return -1;
	}
}
