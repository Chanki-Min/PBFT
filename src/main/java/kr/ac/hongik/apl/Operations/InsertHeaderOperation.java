package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.Blockchain.BlockHeader;
import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;

import java.security.PublicKey;
import java.sql.SQLException;
import java.util.List;

public class InsertHeaderOperation extends Operation {
	private final String chainName;
	private final int batchSize = 100;
	private List<String> hashes;
	private String root;

	public InsertHeaderOperation(PublicKey clientInfo, String chainName, List<String> hashes, String root) {
		super(clientInfo);
		this.chainName = chainName;
		this.hashes = hashes;
		this.root = root;
	}

	public InsertHeaderOperation(PublicKey clientInfo, String chainName, String root) {
		super(clientInfo);
		this.chainName = chainName;
		this.hashes = null;
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

			if (!logger.isBlockChainExists(chainName)) {
				logger.createBlockChainDb(chainName);
			}
			int indexNum = storeHeaderAndReturnIdx(root, logger);
			//hashes가 존재한다면 해쉬 리스트도 넣어준다
			if (hashes != null) {
				storeHash(indexNum, hashes, logger);
			}
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
		String query = "INSERT INTO " + Logger.BLOCK_CHAIN + " VALUES ( ?, ?, ?, ? )";
		try (var psmt = logger.getPreparedStatement(chainName, query)) {
			BlockHeader previousBlock = logger.getLatestBlockHeader(chainName);
			Replica.msgDebugger.debug(String.format("previousBlock : %d", previousBlock.getBlockNumber()));
			String prevHash = Util.hash(previousBlock.toString());

			psmt.setInt(1, previousBlock.getBlockNumber() + 1);
			psmt.setString(2, root);
			psmt.setString(3, prevHash);
			psmt.setBoolean(4, hashes != null);
			psmt.execute();
			return previousBlock.getBlockNumber() + 1;
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
		String query = "INSERT INTO " + Logger.HASHES +" (idx, hash) VALUES( ?, ?)";
		long time = System.currentTimeMillis();
		try (var psmt = logger.getPreparedStatement(chainName, query)) {
			psmt.setInt(1, indexNum);
			psmt.setString(2, Util.serToBase64String(hashes));
			psmt.execute();
			Replica.msgDebugger.info(String.format("hashList insertion end with time : %d", System.currentTimeMillis() - time));
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}
}
