package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import java.security.PublicKey;
import java.sql.SQLException;

public class InsertHeaderOperation extends Operation {
	private final String tableName = "BlockChain";
	private int blockNumber;
	private String root;

	public InsertHeaderOperation(PublicKey clientInfo, int blockNumber, String root) {
		super(clientInfo);
		this.blockNumber = blockNumber;
		this.root = root;
	}

	/**
	 * @param obj Logger
	 * @return indexNum that PBFT actually insert to db
	 */
	@Override
	public Object execute(Object obj) throws SQLException {
		int indexNum = -1;
			Logger logger = (Logger) obj;
			createTableIfNotExists(logger);
			indexNum = storeHeaderAndReturnIdx(blockNumber, root, logger);
		return indexNum;
	}

	/**
	 * createTableIfNotExists creates a BlockChain table and insert a dummy row.
	 *
	 * @param logger
	 */
	private void createTableIfNotExists(Logger logger) {
		try {
			logger.getPreparedStatement("CREATE TABLE " + tableName + " (idx INT, root TEXT, prev TEXT, PRIMARY KEY (idx, root, prev)) ").execute();
			logger.getPreparedStatement("INSERT INTO " + tableName + " VALUES (0, 'FIRST_ROOT', 'PREV')").execute();
		} catch (SQLException ignored) {
		}
	}

	/**
	 * @param root   root is a root of merkle tree
	 * @param logger
	 * @return An index of just inserted block.
	 * @throws SQLException
	 */
	private int storeHeaderAndReturnIdx(int blockNumber, String root, Logger logger) throws SQLException {
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
