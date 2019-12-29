package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.*;
import kr.ac.hongik.apl.Operations.Operation;

import java.io.File;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.List;
import java.util.UUID;

import static kr.ac.hongik.apl.Util.desToObject;
import static kr.ac.hongik.apl.Util.serToBase64String;

public class Logger {
	static final int CONSTRAINT_ERROR = 19;
	private Connection conn = null;
	private String fileName;

	public Logger() {
		this(UUID.randomUUID().toString() + ".db");
	}


	public Logger(String fileName) {
		if (!fileName.endsWith(".db"))
			fileName += ".db";
		this.fileName = fileName;
		try {
			var filePath = getFilePath();
			if (new File(getFilePath().replace("jdbc:sqlite:", "")).exists()) {
				this.conn = DriverManager.getConnection(filePath);
			} else {
				this.conn = DriverManager.getConnection(filePath);
				this.createTables();
			}
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private String getFilePath() {
		var folder = getResourceFolder();
		String encodedPath = "jdbc:sqlite:" + folder + fileName;
		return URLDecoder.decode(encodedPath, StandardCharsets.UTF_8);
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
	private void createTables() {
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
				//BlockChain Table Schema : "(idx INT, root TEXT, prev TEXT, PRIMARY KEY (idx, root, prev))"
				"CREATE TABLE BlockChain (idx INT, root TEXT, prev TEXT, PRIMARY KEY (idx, root, prev))",
				"INSERT INTO BlockChain VALUES (0, 'FIRST_ROOT', 'PREV')",

				"CREATE TABLE Hashes (idx INT, hash TEXT, PRIMARY KEY(idx) )"
		};
		for (String query: queries) {
			try (var preparedStatement = conn.prepareStatement(query)) {
				preparedStatement.execute();
			} catch (SQLException e) {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
	}

	private String getResourceFolder() {
		var folder = System.getProperty("user.dir");
		return folder.endsWith("/") ? folder : folder + '/';
	}

	void close() {
		try {
			conn.close();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	public String getStateDigest(int seqNum, int maxFaulty, int viewNum) {
		StringBuilder builder = new StringBuilder();

		String baseQuery = "SELECT lastStableCheckpoint, seqNum, digest FROM UnstableCheckPoints";
		try (var psmt = conn.prepareStatement(baseQuery)) {
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
		try (var pstmt = conn.prepareStatement(baseQuery)) {
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

		try (var pstmt = conn.prepareStatement(baseQuery)) {
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

		try (var pstmt = conn.prepareStatement(baseQuery)) {
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

		try (var psmt = conn.prepareStatement(query.toString())) {
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
		try (var pstmt = conn.prepareStatement(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private void cleanUpPrepareMsg(int seqNum) {
		String query = "DELETE FROM Prepares WHERE seqNum <= ?";
		try (var pstmt = conn.prepareStatement(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private void cleanUpCommitMsg(int seqNum) {
		String query = "DELETE FROM Commits WHERE seqNum <= ?";
		try (var pstmt = conn.prepareStatement(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}
	private void cleanUpUnstableCheckpoint(int seqNum) {
		String query = "DELETE FROM UnstableCheckPoints WHERE seqNum <= ?";
		try (var pstmt = conn.prepareStatement(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.execute();
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}
	private void cleanUpCheckpointMsg(int seqNum) {
		String query = "DELETE FROM Checkpoints WHERE seqNum < ?";
		try (var pstmt = conn.prepareStatement(query)) {
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
		try (var pstmt = getPreparedStatement(baseQuery)) {
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

	public PreparedStatement getPreparedStatement(String baseQuery) {
		try {
			return conn.prepareStatement(baseQuery);
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
		try (var pstmt = conn.prepareStatement(baseQuery)){
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
		try (PreparedStatement pstmt = conn.prepareStatement(baseQuery);){
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
		try (PreparedStatement preparedStatement = conn.prepareStatement(baseQuery)) {

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
		try (PreparedStatement preparedStatement = conn.prepareStatement(baseQuery)) {

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
		try (PreparedStatement preparedStatement = conn.prepareStatement(baseQuery)) {

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
		try (PreparedStatement preparedStatement = conn.prepareStatement(baseQuery)) {

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
		try (var pstmt = getPreparedStatement(query)) {
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
		try (var pstmt = getPreparedStatement(query)) {
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

	void insertMessage(int sequenceNumber, ReplyMessage message) {
		insertReplyMessage(sequenceNumber, message);
	}

	private void insertReplyMessage(int seqNum, ReplyMessage message) {
		String insertQuery = "INSERT INTO Executed VALUES (?, ?, ?)";
		String clientInfo = Util.serToBase64String(message.getClientInfo());
		try (var psmt = getPreparedStatement(insertQuery)) {
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
		try (var psmt = getPreparedStatement(deleteQuery)) {
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
		try (var psmt = conn.prepareStatement(baseQuery)) {

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
		try (var psmt = conn.prepareStatement(baseQuery)) {
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
		try (var psmt = conn.prepareStatement(baseQuery)) {
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
		try (var pstatement = conn.prepareStatement(baseQuery)) {
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
}
