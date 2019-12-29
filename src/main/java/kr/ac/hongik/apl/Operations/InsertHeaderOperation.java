package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.security.PublicKey;
import java.sql.SQLException;
import java.util.List;

public class InsertHeaderOperation extends Operation {
	private final String tableName = "BlockChain";
	private final int batchSize = 100;
	private List<String> hashes;
	private String root;

	public InsertHeaderOperation(PublicKey clientInfo, List<String> hashes, String root) {
		super(clientInfo);
		this.hashes = hashes;
		this.root = root;
	}

	/**
	 * @param obj Logger
	 * @return indexNum that PBFT actually insert to db
	 */
	@Override
	public Object execute(Object obj) throws OperationExecutionException {
		try {
			Logger logger = (Logger) obj;
			int indexNum = storeHeaderAndReturnIdx(root, logger);
			storeHash(indexNum, hashes, logger);
			return indexNum;
		} catch (Exception e) {
			throw new OperationExecutionException(e);
		}
	}

	/**
	 * @param root   root is a root of merkle tree
	 * @param logger logger
	 * @return An index of just inserted block.
	 * @throws SQLException
	 */
	private int storeHeaderAndReturnIdx(String root, Logger logger) {
		String query = "INSERT INTO " + tableName + " VALUES ( ?, ?, ? )";
		try (var psmt = logger.getPreparedStatement(query)) {
			Triple<Integer, String, String> previousBlock = getLatestBlock(logger);
			Replica.msgDebugger.debug(String.format("previousBlock : %d", previousBlock.getLeft()));
			String prevHash = Util.hash(previousBlock.toString());

			psmt.setInt(1, previousBlock.getLeft() + 1);
			psmt.setString(2, root);
			psmt.setString(3, prevHash);
			psmt.execute();
			return previousBlock.getLeft() + 1;
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * @param indexNum hashes's index number
	 * @param hashes list of all entry's hash
	 * @param logger logger
	 * @throws SQLException
	 */
	private void storeHash(int indexNum, List<String> hashes, Logger logger) {
		String query = "INSERT INTO Hashes (idx, hash) VALUES( ?, ?)";
		long time = System.currentTimeMillis();
		try (var psmt = logger.getPreparedStatement(query)) {
			psmt.setInt(1, indexNum);
			psmt.setString(2, Util.serToBase64String(hashes));
			psmt.execute();
			Replica.msgDebugger.info(String.format("hashlist insertion end with time : %d", System.currentTimeMillis() - time));
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * @param logger
	 * @return A triplet of (Block number, merkle root, previous block hash)
	 * @throws SQLException
	 */
	private Triple<Integer, String, String> getLatestBlock(Logger logger) throws SQLException {
		String query = "SELECT idx, root, prev FROM " + tableName + " b WHERE b.idx = (SELECT MAX(idx) from " + tableName + " b2)";
		try (var psmt = logger.getPreparedStatement(query)) {
			var ret = psmt.executeQuery();
			if (ret.next())
				return new ImmutableTriple<>(ret.getInt(1), ret.getString(2), ret.getString(3));
			else
				throw new SQLException("There's no tuple in " + tableName);
		}
	}
}
