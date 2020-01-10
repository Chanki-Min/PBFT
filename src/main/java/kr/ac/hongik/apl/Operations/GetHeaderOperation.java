package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.Logger;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.security.PublicKey;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetHeaderOperation extends Operation {
	private String chainName;
	private int blockNumber;

	public GetHeaderOperation(PublicKey clientInfo, String chainName, int blockNumber) {
		super(clientInfo);
		this.chainName = chainName;
		this.blockNumber = blockNumber;
	}

	@Override
	public Object execute(Object obj) throws OperationExecutionException {
		try {
			if (blockNumber == 0) {
				return new ImmutableTriple<Integer, String, String>(0, "FIRST_ROOT", "PREV");
			}
			Logger logger = (Logger) obj;
			String baseQuery = "SELECT b.idx, b.root, b.prev FROM " + Logger.BLOCK_CHAIN +" AS b WHERE b.idx = ?";
			try (var psmt = logger.getPreparedStatement(chainName, baseQuery)) {
				psmt.setInt(1, blockNumber);
				ResultSet ret = psmt.executeQuery();

				Triple<Integer, String, String> header = null;
				if (ret.next()) {
					header = new ImmutableTriple<Integer, String, String>(
							ret.getInt("idx"),
							ret.getString("root"),
							ret.getString("prev")
					);
				}
				return header;
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		} catch (Exception e) {
			throw new OperationExecutionException(e);
		}
	}
}
