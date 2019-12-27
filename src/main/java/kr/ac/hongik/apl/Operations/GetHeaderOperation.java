package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Replica;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetHeaderOperation extends Operation {
	private int blockNumber;

	public GetHeaderOperation(PublicKey clientInfo, int blockNumber) {
		super(clientInfo);
		this.blockNumber = blockNumber;
	}

	@Override
	public Object execute(Object obj) {
		try {
			if (blockNumber == 0) {
				return new ImmutableTriple<Integer, String, String>(0, "FIRST_ROOT", "PREV");
			}
			Logger logger = (Logger) obj;
			String baseQuery = "SELECT b.idx, b.root, b.prev FROM BlockChain AS b WHERE b.idx = " + blockNumber;
			PreparedStatement pstmt = logger.getPreparedStatement(baseQuery);
			ResultSet ret = pstmt.executeQuery();

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
			Replica.msgDebugger.error(e.getMessage());
			return null;
		}
	}
}
