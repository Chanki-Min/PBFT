package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.Logger;

import java.security.PublicKey;
import java.sql.SQLException;

public class GetLatestBlockNumberOperation extends Operation {
	public GetLatestBlockNumberOperation(PublicKey clientInfo) {
		super(clientInfo);
	}

	@Override
	public Object execute(Object obj) throws OperationExecutionException {
		try {
			Logger logger = (Logger) obj;
			String query = "SELECT max(idx) from BlockChain";
			try (var psmt = logger.getPreparedStatement(query)) {
				var rs = psmt.executeQuery();
				if (rs.next()) {
					return rs.getInt(1);
				} else {
					return -1;
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		} catch (Exception e) {
			throw new OperationExecutionException(e);
		}
	}
}
