package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.Logger;

import java.security.PublicKey;
import java.sql.SQLException;

public class GetLatestBlockNumberOperation extends Operation {
	private String chainName;

	public GetLatestBlockNumberOperation(PublicKey clientInfo, String chainName) {
		super(clientInfo);
		this.chainName = chainName;
	}

	@Override
	public Object execute(Object obj) throws OperationExecutionException {
		try {
			Logger logger = (Logger) obj;
			String query = "SELECT max(idx) from " + Logger.BLOCK_CHAIN;
			try (var psmt = logger.getPreparedStatement(chainName, query)) {
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
