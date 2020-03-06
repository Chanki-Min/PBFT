package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Blockchain.BlockHeader;
import kr.ac.hongik.apl.Messages.*;
import kr.ac.hongik.apl.Operations.Operation;

import java.io.File;
import java.io.FilenameFilter;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static kr.ac.hongik.apl.Util.desToObject;
import static kr.ac.hongik.apl.Util.serToBase64String;

public class Logger {
	static final int CONSTRAINT_ERROR = 19;
	public static final String CONSENSUS = "consensus";
	public static final String BLOCK_CHAIN = "BlockChain";
	public static final String HASHES = "Hashes";
	private static final String ABSOLUTE_DB_NAME = "/replicaData";
	public final String DB_PATH;

	private Map<String, Connection> connectionMap = new HashMap<>();

	public Logger(String serverIP, int serverPort) {
		DB_PATH = String.format("%s_%s_%d/", ABSOLUTE_DB_NAME, serverIP, serverPort);

		createDataDirectoryIfNotExists();
		loadDbConnection();
		if (!connectionMap.containsKey(Logger.CONSENSUS)) {
			createConsensusDb();
		}
	}

	public void loadDbConnection() {
		File dataDir = new File(getResourceFolder());
		if (!dataDir.exists() || !dataDir.isDirectory()) {
			throw new NoSuchElementException(String.format("cannt find dbDir : %s", getResourceFolder()));
		}

		String[] dbFileList = dataDir.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith("db");
			}
		});
		assert dbFileList != null;

		for (String dbFile: dbFileList) {
			var urlDbFile = getFilePath(dbFile);
			try {
				String key = dbFile.replace(".db", "");

				if (!connectionMap.containsKey(key)) {
					var conn = DriverManager.getConnection(urlDbFile);
					connectionMap.put(key, conn);
				} else {
					Replica.msgDebugger.warn(String.format("duplicated db connection ignored, fileURL : %s", urlDbFile));
				}
			} catch (SQLException e) {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
	}

	private void createConsensusDb() {
		String consensusDbFileName = Logger.CONSENSUS + ".db";
		try {
			String consensusFilePath = getFilePath(consensusDbFileName);
			Connection conn;
			if (new File(getFilePath(consensusDbFileName).replace("jdbc:sqlite:", "")).exists()) {
				conn = DriverManager.getConnection(consensusFilePath);
				connectionMap.put(CONSENSUS, conn);
			} else {
				conn = DriverManager.getConnection(consensusFilePath);
				connectionMap.put(CONSENSUS, conn);
				this.createConsensusTables();
			}
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	public void createBlockChainDb(String chainName) {
		String chainFileName = chainName;

		if (!chainName.endsWith(".db")) {
			chainFileName += ".db";
		}
		try {
			String chainFilePath = getFilePath(chainFileName);
			if (new File(getFilePath(chainFileName).replace("jdbc:sqlite:", "")).exists()) {
				throw new DuplicatedDbFileException(String.format("chainName : %s, already exists", chainFilePath));
			}

			var conn = DriverManager.getConnection(chainFilePath);
			connectionMap.put(chainName, conn);
			this.createBlockChainTables(chainName);
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private String getResourceFolder() {
		//String folder = System.getProperty("user.dir");
		String folder = Util.getCurrentProgramDir();
		folder = folder.endsWith("/") ? folder.substring(0, folder.length() - 1) : folder;
		return folder.concat(DB_PATH);
	}

	private String getFilePath(String fileName) {
		String folder = getResourceFolder();
		String encodedPath = "jdbc:sqlite:" + folder + fileName;
		return URLDecoder.decode(encodedPath, StandardCharsets.UTF_8);
	}

	private void createDataDirectoryIfNotExists() {
		String dataDir = getResourceFolder();
		var dir = new File(dataDir);
		if (!dir.exists())
			dir.mkdir();
	}

	/**
	 * Schema Design
	 * ^ symbol means it is a prime attribute.
	 * Requests:    (^client, ^timestamp, ^operation). Insert this triple after its signature is validated. Primary only.
	 * Preprepares: (^viewNum, ^seqNum, ^digest, operation)
	 * Prepares:    (^viewNum, ^seqNum, ^digest, ^i)
	 * Commits:     (viewNum, ^seqNum, digest, ^i)
	 * Checkpoints: (^seqNum, ^stateDigest, ^i)
	 * Executed: (^seqNum, replyMessage)
	 * <p>
	 * Blob insertion and comparison won't work so We are going to use serialized Base64 encoding instead of Blob
	 */
	private void createConsensusTables() {
		String[] queries = {
				"CREATE TABLE Requests (client TEXT,timestamp DATE,operation TEXT, PRIMARY KEY(client, timestamp, operation))",
				"CREATE TABLE Preprepares (viewNum INT, seqNum INT, digest TEXT, requestMessage TEXT, data TEXT, " +
						"PRIMARY " +
						"KEY" +
						"(viewNum, seqNum, digest))",
				"CREATE TABLE Prepares (viewNum INT, seqNum INT, digest TEXT, replica INT, data TEXT, PRIMARY KEY" +
						"(viewNum, " +
						"seqNum, digest, replica))",
				"CREATE TABLE Commits (viewNum INT, seqNum INT, digest TEXT, replica INT, PRIMARY KEY(seqNum, replica, digest))",
				"CREATE TABLE Checkpoints (seqNum INT, stateDigest TEXT, replica INT,data TEXT, PRIMARY KEY(seqNum, " +
						"stateDigest, replica))",
				"CREATE TABLE UnstableCheckPoints (lastStableCheckpoint INT, seqNum INT, digest TEXT, PRIMARY KEY(lastStableCheckpoint, seqNum, digest))",
				"CREATE TABLE Executed (client TEXT, seqNum INT, replyMessage TEXT NOT NULL, PRIMARY KEY(seqNum))",
				"CREATE TABLE ViewChanges (newViewNum INT, checkpointNum INT, replica INT, checkpointMsgs TEXT, PPMsgs TEXT, data TEXT, " +
						"PRIMARY KEY(newViewNum, replica))",
				"CREATE TABLE NewViewMessages (newViewNum INT, data TEXT, PRIMARY KEY(newViewNum) )",
				"CREATE TABLE LatestHeaders (chainName TEXT, idx INT, root TEXT, prev TEXT, hasHashList BOOLEAN, PRIMARY KEY(chainName) )",
		};
		for (String query: queries) {
			try (var preparedStatement = connectionMap.get(CONSENSUS).prepareStatement(query)) {
				preparedStatement.execute();
			} catch (SQLException e) {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
	}

	private void createBlockChainTables(String chainName) {
		String [] queries = {
				"CREATE TABLE BlockChain (idx INT, root TEXT, prev TEXT, hasHashList BOOLEAN, PRIMARY KEY (idx))",
				"INSERT INTO BlockChain VALUES (0, 'FIRST_ROOT', 'PREV', 0)",

				"CREATE TABLE Hashes (idx INT, hash TEXT, PRIMARY KEY(idx) )"
		};
		for (String query: queries) {
			try (var preparedStatement = connectionMap.get(chainName).prepareStatement(query)) {
				preparedStatement.execute();
			} catch (SQLException e) {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}

	}

	public boolean isBlockChainExists(String dbName) {
		return connectionMap.containsKey(dbName);
	}

	public List<String> getLoadedChainList() {
		return connectionMap.keySet().stream()
				.filter(x -> !x.equals(CONSENSUS))
				.collect(Collectors.toUnmodifiableList());
	}

	/**
	 * @return A triplet of (Block number, merkle root, previous block hash)
	 * @throws SQLException
	 */
	public BlockHeader getLatestBlockHeader(String chainName) {
		String query = "SELECT idx, root, prev, hasHashList FROM " + Logger.BLOCK_CHAIN + " b WHERE b.idx = (SELECT MAX(idx) from " + Logger.BLOCK_CHAIN + " b2)";
		try (var psmt = this.getPreparedStatement(chainName, query)) {
			var ret = psmt.executeQuery();
			if (ret.next())
				return new BlockHeader(
						ret.getInt(1), ret.getString(2),
						ret.getString(3), ret.getBoolean(4)
				);
			else
				throw new SQLException(String.format("cannot find latest blockHeader from chain : %s", chainName));
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * @return A triplet of (Block number, merkle root, previous block hash)
	 * @throws SQLException
	 */
	public BlockHeader getBlockHeader(String chainName, int blockNumber) {
		String query = "SELECT idx, root, prev, hasHashList FROM " + Logger.BLOCK_CHAIN + " b WHERE b.idx = ?";
		try (var psmt = this.getPreparedStatement(chainName, query)) {
			psmt.setInt(1, blockNumber);
			var ret = psmt.executeQuery();

			if (ret.next())
				return new BlockHeader(
						ret.getInt(1), ret.getString(2),
						ret.getString(3), ret.getBoolean(4)
				);
			else
				throw new SQLException(String.format("cannot find blockHeader from chain : %s, blockNumber : %d", chainName, blockNumber));
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	//TODO : Logger::close() 메소드 구현
	/*
	void close() {
		try {
			conn.close();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}
	ㅇ
	 */

	public String getStateDigest(int seqNum, int maxFaulty, int viewNum) {
		StringBuilder builder = new StringBuilder();

		String baseQuery = "SELECT lastStableCheckpoint, seqNum, digest FROM UnstableCheckPoints";
		try (var psmt = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)) {
			ResultSet ret = psmt.executeQuery();
			while (ret.next()) {
				builder.append(ret.getInt(1));
				builder.append(ret.getInt(2));
				builder.append(ret.getString(3));
			}
			return Util.hash(builder.toString().getBytes());
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private String getPrePrepareMsgs(int seqNum, int viewNum) {
		String baseQuery = "SELECT DISTINCT digest, viewNum, seqNum FROM Preprepares WHERE seqNum <= ? AND viewNum = ? ORDER BY seqNum";
		try (var pstmt = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)) {
			pstmt.setInt(1, seqNum);
			pstmt.setInt(2, viewNum);
			ResultSet ret = pstmt.executeQuery();

			StringBuilder builder = new StringBuilder();
			while (ret.next()) {
				builder.append(ret.getString("digest"));
				builder.append(ret.getInt("viewNum"));
				builder.append(ret.getInt("seqNum"));
			}
			return String.valueOf(builder);
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}

	}

	private String getPrepareMsgs(int seqNum, int maxFaulty, int viewNum) {
		String baseQuery = "SELECT DISTINCT digest, viewNum, seqNum FROM Prepares WHERE seqNum <= ? AND viewNum = ? GROUP BY digest," +
				" viewNum, seqNum HAVING count(*) > ? ORDER BY seqNum";

		try (var pstmt = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)) {
			pstmt.setInt(1, seqNum);
			pstmt.setInt(2, viewNum);
			pstmt.setInt(3, 2 * maxFaulty);


			ResultSet ret = pstmt.executeQuery();

			StringBuilder builder = new StringBuilder();

			while (ret.next()) {
				builder.append(ret.getString("digest"));
				builder.append(ret.getInt("viewNum"));
				builder.append(ret.getInt("seqNum"));
			}
			return String.valueOf(builder);
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}

	}

	private String getCommitMsgs(int seqNum, int maxFaulty) {
		String baseQuery = "SELECT DISTINCT digest, seqNum FROM Commits WHERE seqNum <= ?  GROUP BY digest, seqNum " +
				"HAVING count(*) > ? ORDER BY seqNum";

		try (var pstmt = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)) {
			pstmt.setInt(1, seqNum);
			pstmt.setInt(2, 2 * maxFaulty);
			ResultSet ret = pstmt.executeQuery();

			StringBuilder builder = new StringBuilder();

			while (ret.next()) {
				builder.append(ret.getString(1));
				builder.append(ret.getInt(2));
			}

			return String.valueOf(builder);
		}catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}

	}

	public void executeGarbageCollection(int seqNum, List<String> clientInfoArray) {
		cleanUpPrePrepareMsg(seqNum);
		cleanUpPrepareMsg(seqNum);
		cleanUpCommitMsg(seqNum);
		cleanUpUnstableCheckpoint(seqNum);
		cleanUpCheckpointMsg(seqNum);
		cleanUpExecutedMsg(seqNum-1, clientInfoArray);
	}

	private void cleanUpExecutedMsg(int seqNum, List<String> clientInfoArray) {
		StringBuilder query = new StringBuilder();
		query.append("DELETE FROM Executed WHERE seqNum <= ? AND client NOT IN (");

		for( int i = 0 ; i < clientInfoArray.size(); i++ ) {
			query = i < (clientInfoArray.size() - 1) ? query.append("?,") : query.append("?");
		}
		query.append(")");

		try (var psmt = connectionMap.get(CONSENSUS).prepareStatement(query.toString())) {
			psmt.setInt(1, seqNum);
			for(int i=0; i<clientInfoArray.size(); i++) {
				psmt.setString(i+2, clientInfoArray.get(i));
			}
			psmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private void cleanUpPrePrepareMsg(int seqNum) {
		String query = "DELETE FROM Preprepares WHERE seqNum <= ?";
		try (var pstmt = connectionMap.get(CONSENSUS).prepareStatement(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private void cleanUpPrepareMsg(int seqNum) {
		String query = "DELETE FROM Prepares WHERE seqNum <= ?";
		try (var pstmt = connectionMap.get(CONSENSUS).prepareStatement(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private void cleanUpCommitMsg(int seqNum) {
		String query = "DELETE FROM Commits WHERE seqNum <= ?";
		try (var pstmt = connectionMap.get(CONSENSUS).prepareStatement(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}
	private void cleanUpUnstableCheckpoint(int seqNum) {
		String query = "DELETE FROM UnstableCheckPoints WHERE seqNum <= ?";
		try (var pstmt = connectionMap.get(CONSENSUS).prepareStatement(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}
	private void cleanUpCheckpointMsg(int seqNum) {
		String query = "DELETE FROM Checkpoints WHERE seqNum < ?";
		try (var pstmt = connectionMap.get(CONSENSUS).prepareStatement(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	//new-view-msg 에서 digest(null)이 올 때 의도한 대로 null이 리턴되는지 확인이 필요함.
	Operation getOperation(CommitMessage message) {
		String baseQuery = new StringBuilder()
				.append("SELECT P.requestMessage ")
				.append("FROM Preprepares AS P ")
				.append("WHERE P.seqNum = ? AND P.digest = ?")
				.toString();
		try (var pstmt = getPreparedStatement(CONSENSUS, baseQuery)) {
			pstmt.setInt(1, message.getSeqNum());
			pstmt.setString(2, message.getDigest());

			try (var ret = pstmt.executeQuery()) {
				ret.next();
				String data = ret.getString(1);
				Operation operation = desToObject(data, RequestMessage.class).getOperation();
				return operation;
			}
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	public PreparedStatement getPreparedStatement(String dbName, String baseQuery) {
		try {
			if(!connectionMap.containsKey(dbName))
				throw new SQLException(String.format("no such dbName : %s", dbName));
			else
				return connectionMap.get(dbName).prepareStatement(baseQuery);
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	public void insertMessage(Message message) {
		if (message instanceof RequestMessage) {
			insertRequestMessage((RequestMessage) message);
		} else if (message instanceof PreprepareMessage) {
			insertPreprepareMessage((PreprepareMessage) message);
		} else if (message instanceof PrepareMessage) {
			insertPrepareMessage((PrepareMessage) message);
		} else if (message instanceof CommitMessage) {
			insertCommitMessage((CommitMessage) message);
		} else if(message instanceof UnstableCheckPoint){
			insertNewUnstableCheckPoint((UnstableCheckPoint) message);
		} else if (message instanceof CheckPointMessage) {
			insertCheckPointMessage((CheckPointMessage) message);
		} else if (message instanceof ViewChangeMessage) {
			insertViewChangeMessage((ViewChangeMessage) message);
		} else if (message instanceof NewViewMessage) {
			insertNewViewMessage((NewViewMessage) message);
		} else
			throw new RuntimeException("Invalid message type");
	}

	private void insertRequestMessage(RequestMessage message) {
		String baseQuery = "INSERT INTO Requests (client, timestamp, operation) VALUES ( ?, ?, ? )";
		try (var pstmt = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)){
			String clientInfo = Util.serToBase64String(message.getClientInfo());
			String digest = Util.serToBase64String(message.getOperation());

			pstmt.setString(1, clientInfo);
			pstmt.setLong(2, message.getTime());
			pstmt.setString(3, digest);

			pstmt.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			else {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
	}

	private void insertPreprepareMessage(PreprepareMessage message) {
		String baseQuery = "INSERT INTO Preprepares VALUES ( ?, ?, ?, ?, ?)";
		try (PreparedStatement pstmt = connectionMap.get(CONSENSUS).prepareStatement(baseQuery);){
			pstmt.setInt(1, message.getViewNum());
			pstmt.setInt(2, message.getSeqNum());
			pstmt.setString(3, message.getDigest());
			String data = Util.serToBase64String(message.getRequestMessage());
			pstmt.setString(4, data);
			pstmt.setString(5, Util.serToBase64String(message));

			pstmt.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			else {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
	}

	private void insertPrepareMessage(PrepareMessage message) {
		String baseQuery = "INSERT INTO Prepares VALUES ( ?, ?, ?, ?, ?)";
		try (PreparedStatement preparedStatement = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)) {

			preparedStatement.setInt(1, message.getViewNum());
			preparedStatement.setInt(2, message.getSeqNum());
			preparedStatement.setString(3, message.getDigest());
			preparedStatement.setInt(4, message.getReplicaNum());
			preparedStatement.setString(5, Util.serToBase64String(message));

			preparedStatement.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			else {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
	}

	private void insertCommitMessage(CommitMessage message) {
		String baseQuery = "INSERT INTO Commits VALUES ( ?, ?, ?, ? )";
		try (PreparedStatement preparedStatement = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)) {

			preparedStatement.setInt(1, message.getViewNum());
			preparedStatement.setInt(2, message.getSeqNum());
			preparedStatement.setString(3, message.getDigest());
			preparedStatement.setInt(4, message.getReplicaNum());

			preparedStatement.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			else {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
	}
	private void insertNewUnstableCheckPoint(UnstableCheckPoint unstableCheckPoint){
		String baseQuery = "INSERT INTO UnstableCheckPoints VALUES (? , ? , ?)";
		try (PreparedStatement preparedStatement = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)) {

			preparedStatement.setInt(1, unstableCheckPoint.getLastStableCheckPointNum());
			preparedStatement.setInt(2, unstableCheckPoint.getSeqNum());
			preparedStatement.setString(3, unstableCheckPoint.getDiget());

			preparedStatement.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			else {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
	}
	private void insertCheckPointMessage(CheckPointMessage message) {
		String baseQuery = "INSERT INTO Checkpoints VALUES (? , ? , ?, ?)";
		try (PreparedStatement preparedStatement = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)) {

			preparedStatement.setInt(1, message.getSeqNum());
			preparedStatement.setString(2, message.getDigest());
			preparedStatement.setInt(3, message.getReplicaNum());
			preparedStatement.setString(4, Util.serToBase64String(message));

			preparedStatement.execute();

		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			else {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
	}

	private void insertViewChangeMessage(ViewChangeMessage message) {
		String query = "INSERT INTO ViewChanges VALUES (?, ?, ?, ?, ?, ?)";
		try (var pstmt = getPreparedStatement(CONSENSUS, query)) {
			pstmt.setInt(1, message.getNewViewNum());
			pstmt.setInt(2, message.getLastCheckpointNum());
			pstmt.setInt(3, message.getReplicaNum());
			pstmt.setString(4, serToBase64String(message.getCheckPointMessages()));
			pstmt.setString(5, serToBase64String(message.getMessageList()));
			pstmt.setString(6, Util.serToBase64String(message));

			pstmt.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			else {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
	}

	private void insertNewViewMessage(NewViewMessage message) {
		String query = "INSERT INTO NewViewMessages VALUES (?, ?)";
		try (var pstmt = getPreparedStatement(CONSENSUS, query)) {
			pstmt.setInt(1, message.getNewViewNum());
			pstmt.setString(2, Util.serToBase64String(message));

			pstmt.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			else {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
	}

	public void insertBlockChain(String chainName, BlockHeader blockHeader) {
		String query = "INSERT INTO " + Logger.BLOCK_CHAIN + " VALUES ( ?, ?, ?, ? )";
		try (var psmt = getPreparedStatement(chainName, query)) {
			psmt.setInt(1, blockHeader.getBlockNumber());
			psmt.setString(2, blockHeader.getRootHash());
			psmt.setString(3, blockHeader.getPrevHash());
			psmt.setBoolean(4, blockHeader.getHasHashList() != null);
			psmt.execute();
		} catch (SQLException e) {
			if(e.getErrorCode() == CONSTRAINT_ERROR) {
				Replica.msgDebugger.warn(String.format("got CONSTRAINT_ERROR while inserting %s. Updating %s.", BLOCK_CHAIN, chainName));
				deleteBlockChain(chainName, blockHeader.getBlockNumber());
				insertBlockChain(chainName, blockHeader);
			} else {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
	}

	private void deleteBlockChain(String chainName, int idx) {
		String query = "DELETE FROM " + Logger.BLOCK_CHAIN + " WHERE idx = ?";
		try(var psmt = getPreparedStatement(chainName, query)) {
			psmt.setInt(1, idx);
			psmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	public void updateLatestHeaders() {
		Map<String, BlockHeader> latestHeaderMap = new HashMap<>();
		getLoadedChainList().forEach(chain -> latestHeaderMap.put(chain, getLatestBlockHeader(chain)));

		String deleteQuery = "DELETE FROM LatestHeaders";
		try (var psmt = getPreparedStatement(CONSENSUS, deleteQuery)) {
			psmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}

		String query = "INSERT INTO LatestHeaders VALUES (?, ?, ?, ?, ?)";
		try (var psmt = getPreparedStatement(CONSENSUS, query)) {
			for (Map.Entry<String, BlockHeader> entry: latestHeaderMap.entrySet()) {
				psmt.setString(1, entry.getKey());
				psmt.setInt(2, entry.getValue().getBlockNumber());
				psmt.setString(3, entry.getValue().getRootHash());
				psmt.setString(4, entry.getValue().getPrevHash());
				psmt.setBoolean(5, entry.getValue().getHasHashList());
				psmt.addBatch();
			}
			psmt.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public Map<String, BlockHeader> getLatestHeaders() {
		Map<String, BlockHeader> latestHeaders = new HashMap<>();
		String query = "SELECT chainName, idx, root, prev, hasHashList FROM LatestHeaders";
		try (var psmt = getPreparedStatement(CONSENSUS, query)) {
			ResultSet ret = psmt.executeQuery();
			while (ret.next()) {
				BlockHeader blockHeader = new BlockHeader(ret.getInt(2), ret.getString(3), ret.getString(4), ret.getBoolean(5));
				latestHeaders.put(ret.getString(1), blockHeader);
			}
			return latestHeaders;
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}

	}

	void insertMessage(int sequenceNumber, ReplyMessage message) {
		insertReplyMessage(sequenceNumber, message);
	}

	private void insertReplyMessage(int seqNum, ReplyMessage message) {
		String insertQuery = "INSERT INTO Executed VALUES (?, ?, ?)";
		String clientInfo = Util.serToBase64String(message.getClientInfo());
		try (var psmt = getPreparedStatement(CONSENSUS, insertQuery)) {
			String data = Util.serToBase64String(message);
			psmt.setString(1, clientInfo);
			psmt.setInt(2, seqNum);
			psmt.setString(3, data);
			psmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}

		String deleteQuery = "DELETE FROM Executed WHERE client = ? AND seqNum < ?";
		try (var psmt = getPreparedStatement(CONSENSUS, deleteQuery)) {
			psmt.setString(1, clientInfo);
			psmt.setInt(2, seqNum);
			psmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	public boolean findMessage(Message message) {
		if (message instanceof RequestMessage) {
			return findRequestMessage((RequestMessage) message);
		} else if (message instanceof PreprepareMessage) {
			return findPreprepareMessage((PreprepareMessage) message);
		} else if (message instanceof PrepareMessage) {
			return findPrepareMessage((PrepareMessage) message);
		} else if (message instanceof CommitMessage) {
			return findCommitMessage((CommitMessage) message);
		}
		return false;
	}

	private boolean findRequestMessage(RequestMessage message) {
		String baseQuery = "SELECT COUNT(*) FROM Requests R WHERE R.client = ? AND R.timestamp = ? AND R.operation = ?";
		try (var psmt = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)) {

			String data = Util.serToBase64String(message.getClientInfo());
			psmt.setString(1, data);
			psmt.setLong(2, message.getTime());
			String data1 = Util.serToBase64String(message.getOperation());
			psmt.setString(3, data1);

			var ret = psmt.executeQuery();
			if (ret.next())
				return ret.getInt(1) == 1;
			else
				throw new SQLException("Failed to find the message in the DB");
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private boolean findPreprepareMessage(PreprepareMessage message) {
		String baseQuery = "SELECT COUNT(*) FROM Preprepares P WHERE P.viewNum = ? AND P.seqNum = ? AND P.digest = ?";
		try (var psmt = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)) {
			psmt.setInt(1, message.getViewNum());
			psmt.setInt(2, message.getSeqNum());
			psmt.setString(3, message.getDigest());

			var ret = psmt.executeQuery();
			if (ret.next())
				return ret.getInt(1) == 1;
			else
				throw new SQLException("Failed to find the message in the DB");
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private boolean findPrepareMessage(PrepareMessage message) {
		String baseQuery = "SELECT COUNT(*) FROM Prepares P WHERE P.viewNum = ? AND P.seqNum = ? AND P.digest = ? AND P.replica = ?";
		try (var psmt = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)) {
			psmt.setInt(1, message.getViewNum());
			psmt.setInt(2, message.getSeqNum());
			psmt.setString(3, message.getDigest());
			psmt.setInt(4, message.getReplicaNum());

			ResultSet ret = psmt.executeQuery();
			if (ret.next())
				return ret.getInt(1) == 1;
			else
				throw new SQLException("Failed to find the message in the DB");
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private boolean findCommitMessage(CommitMessage message) {
		String baseQuery = "SELECT COUNT(*) FROM Commits C WHERE C.viewNum = ? AND C.seqNum = ? AND C.digest = ? AND C.replica = ?";
		try (var pstatement = connectionMap.get(CONSENSUS).prepareStatement(baseQuery)) {
			pstatement.setInt(1, message.getViewNum());
			pstatement.setInt(2, message.getSeqNum());
			pstatement.setString(3, message.getDigest());
			pstatement.setInt(4, message.getReplicaNum());

			ResultSet ret = pstatement.executeQuery();
			if (ret.next())
				return ret.getInt(1) == 1;
			else
				throw new SQLException("Failed to find the message in the DB");
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	class DuplicatedDbFileException extends RuntimeException {
		DuplicatedDbFileException(String e) {
			super(e);
		}

		DuplicatedDbFileException(Exception e) {
			super(e);
		}
	}
}
