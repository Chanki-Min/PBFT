package kr.ac.hongik.apl.Operations.Dev;

import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Operations.OperationExecutionException;

import java.security.PublicKey;
import java.sql.SQLException;

public class CountMsgsOperation extends Operation {
	public CountMsgsOperation(PublicKey clientInfo) {
		super(clientInfo);
	}

	@Override
	public Object execute(Object obj) throws OperationExecutionException {
		try {
			Logger logger = (Logger) obj;
			String[] table_name = {"Preprepares", "Prepares", "Commits", "Executed"};
			String base_query = "SELECT COUNT(*) FROM ";

			int[] result = new int[table_name.length];

			for (int i = 0; i < result.length; i++) {
				try (var pstmt = logger.getPreparedStatement(Logger.CONSENSUS, base_query + table_name[i])) {
					var ret = pstmt.executeQuery();
					if (ret.next()) {
						result[i] = ret.getInt(1);
					} else {
						result[i] = -987654321;
					}
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			}
			return result;
		} catch (Exception e) {
			throw new OperationExecutionException(e);
		}
	}
}
