package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.*;
import kr.ac.hongik.apl.Operations.Operation;

import java.io.File;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.UUID;

import static kr.ac.hongik.apl.Util.desToObject;
import static kr.ac.hongik.apl.Util.serToString;

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
			e.printStackTrace();
		}
	}

	private String getFilePath() {
		var folder = getResourceFolder();
		String encodedPath = "jdbc:sqlite:" + folder + fileName;
		String url = URLDecoder.decode(encodedPath, StandardCharsets.UTF_8);
		return url;
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
				"CREATE TABLE Commits (viewNum INT, seqNum INT, digest TEXT, replica INT, PRIMARY KEY(seqNum, replica))",
				"CREATE TABLE Checkpoints (seqNum INT, stateDigest TEXT, replica INT,data TEXT, PRIMARY KEY(seqNum, " +
						"stateDigest, replica))",
				"CREATE TABLE Executed (seqNum INT, replyMessage TEXT NOT NULL, PRIMARY KEY(seqNum))",
				"CREATE TABLE ViewChanges (newViewNum INT, checkpointNum INT, replica INT, checkpointMsgs TEXT, PPMsgs TEXT, data TEXT, " +
						"PRIMARY KEY(newViewNum, replica))",
				"CREATE TABLE NewViewMessages (newViewNum INT, data TEXT, PRIMARY KEY(newViewNum) )",

				"CREATE TABLE VerificationLogs (timestamp DATE, blockNum INT, entryNum INT, errorCode TEXT, PRIMARY KEY (timestamp, blockNum, entryNum) )",
				//BlockChain Table Schema : "(idx INT, root TEXT, prev TEXT, PRIMARY KEY (idx, root, prev))"
		};
		for (String query : queries) {
			try {
				PreparedStatement preparedStatement = conn.prepareStatement(query);
				preparedStatement.execute();
			} catch (SQLException e) {
				e.printStackTrace();
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
			e.printStackTrace();
		}
	}

	public String getStateDigest(int seqNum, int maxFaulty, int viewNum) throws SQLException {
		StringBuilder builder = new StringBuilder();
		builder.append(getPrePrepareMsgs(seqNum, viewNum));
		builder.append(getPrepareMsgs(seqNum, maxFaulty, viewNum));
		builder.append(getCommitMsgs(seqNum, maxFaulty));

		return Util.hash(builder.toString().getBytes());
	}

	private String getPrePrepareMsgs(int seqNum, int viewNum) throws SQLException {

		String baseQuery = "SELECT DISTINCT digest, viewNum, seqNum FROM Preprepares WHERE seqNum <= ? AND viewNum = ? ORDER BY seqNum";
		PreparedStatement pstmt = conn.prepareStatement(baseQuery);
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

	}

	private String getPrepareMsgs(int seqNum, int maxFaulty, int viewNum) throws SQLException {
		String baseQuery = "SELECT DISTINCT digest, viewNum, seqNum FROM Prepares WHERE seqNum <= ? AND viewNum = ? GROUP BY digest," +
				" viewNum, seqNum HAVING count(*) > ? ORDER BY seqNum";

		PreparedStatement pstmt = conn.prepareStatement(baseQuery);
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

	}

	private String getCommitMsgs(int seqNum, int maxFaulty) throws SQLException {
		String baseQuery = "SELECT DISTINCT digest, seqNum FROM Commits WHERE seqNum <= ?  GROUP BY digest, seqNum " +
				"HAVING count(*) > ? ORDER BY seqNum";

		PreparedStatement pstmt = conn.prepareStatement(baseQuery);
		pstmt.setInt(1, seqNum);
		pstmt.setInt(2, 2 * maxFaulty);
		ResultSet ret = pstmt.executeQuery();

		StringBuilder builder = new StringBuilder();

		while (ret.next()) {
			builder.append(ret.getString(1));
			builder.append(ret.getInt(2));
		}

		return String.valueOf(builder);

	}

	public void executeGarbageCollection(int seqNum) {
		try {
			cleanUpPrePrepareMsg(seqNum);
			cleanUpPrepareMsg(seqNum);
			cleanUpCommitMsg(seqNum);
			cleanUpCheckpointMsg(seqNum);
		} catch (SQLException e) {
            if (e.getErrorCode() == CONSTRAINT_ERROR)
                return;
			e.printStackTrace();
		}
	}

	private void cleanUpPrePrepareMsg(int seqNum) throws SQLException {
		String query = "DELETE FROM Preprepares WHERE seqNum <= ?";
		PreparedStatement pstmt = conn.prepareStatement(query);
		pstmt.setInt(1, seqNum);
		pstmt.execute();
	}

	private void cleanUpPrepareMsg(int seqNum) throws SQLException {
		String query = "DELETE FROM Prepares WHERE seqNum <= ?";
		PreparedStatement pstmt = conn.prepareStatement(query);
		pstmt.setInt(1, seqNum);
		pstmt.execute();
	}

	private void cleanUpCommitMsg(int seqNum) throws SQLException {
		String query = "DELETE FROM Commits WHERE seqNum <= ?";
		PreparedStatement pstmt = conn.prepareStatement(query);
		pstmt.setInt(1, seqNum);
		pstmt.execute();
	}

	private void cleanUpCheckpointMsg(int seqNum) throws SQLException {
		String query = "DELETE FROM Checkpoints WHERE seqNum < ?";
		PreparedStatement pstmt = conn.prepareStatement(query);
		pstmt.setInt(1, seqNum);
		pstmt.execute();
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
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public PreparedStatement getPreparedStatement(String baseQuery) throws SQLException {
		return conn.prepareStatement(baseQuery);
	}

	public void insertErrorLog(long timeStamp, int blockNum, int entryNum, String errorCode) {
		String baseQuery = "INSERT INTO VerificationLogs (timestamp, blockNum, entryNum, errorCode) VALUES (?, ?, ?, ? )";
		try {
			PreparedStatement psmt = conn.prepareStatement(baseQuery);
			psmt.setLong(1 ,timeStamp);
			psmt.setInt(2, blockNum);
			psmt.setInt(3, entryNum);
			psmt.setString(4, errorCode);

			psmt.execute();
		} catch (SQLException e) {
			e.printStackTrace();
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
		}  else if (message instanceof CheckPointMessage) {
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
		try {
			PreparedStatement pstmt = conn.prepareStatement(baseQuery);

			String data = serToString(message.getClientInfo());
			pstmt.setString(1, data);
			pstmt.setLong(2, message.getTime());
			String data1 = serToString(message.getOperation());
			pstmt.setString(3, data1);

			pstmt.execute();
		} catch (SQLException e) {
			if(e.getErrorCode()==CONSTRAINT_ERROR) return;
			e.printStackTrace();
		}
	}

	private void insertPreprepareMessage(PreprepareMessage message) {
		String baseQuery = "INSERT INTO Preprepares VALUES ( ?, ?, ?, ?, ?)";
		try {
			PreparedStatement pstmt = conn.prepareStatement(baseQuery);

			pstmt.setInt(1, message.getViewNum());
			pstmt.setInt(2, message.getSeqNum());
			pstmt.setString(3, message.getDigest());
			String data = serToString(message.getRequestMessage());
			pstmt.setString(4, data);
			pstmt.setString(5, serToString(message));

			pstmt.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			e.printStackTrace();
		}
	}

	private void insertPrepareMessage(PrepareMessage message) {
		String baseQuery = "INSERT INTO Prepares VALUES ( ?, ?, ?, ?, ?)";
		try {
			PreparedStatement preparedStatement = conn.prepareStatement(baseQuery);

			preparedStatement.setInt(1, message.getViewNum());
			preparedStatement.setInt(2, message.getSeqNum());
			preparedStatement.setString(3, message.getDigest());
			preparedStatement.setInt(4, message.getReplicaNum());
			preparedStatement.setString(5, serToString(message));

			preparedStatement.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			e.printStackTrace();
		}
	}

	private void insertCommitMessage(CommitMessage message) {
		String baseQuery = "INSERT INTO Commits VALUES ( ?, ?, ?, ? )";
		try {
			PreparedStatement preparedStatement = conn.prepareStatement(baseQuery);

			preparedStatement.setInt(1, message.getViewNum());
			preparedStatement.setInt(2, message.getSeqNum());
			preparedStatement.setString(3, message.getDigest());
			preparedStatement.setInt(4, message.getReplicaNum());

			preparedStatement.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			e.printStackTrace();
		}
	}

	private void insertCheckPointMessage(CheckPointMessage message) {
		String baseQuery = "INSERT INTO Checkpoints VALUES (? , ? , ?, ?)";
		try {
			PreparedStatement preparedStatement = conn.prepareStatement(baseQuery);

			preparedStatement.setInt(1, message.getSeqNum());
			preparedStatement.setString(2, message.getDigest());
			preparedStatement.setInt(3, message.getReplicaNum());
			preparedStatement.setString(4, serToString(message));

			preparedStatement.execute();

		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			e.printStackTrace();
		}
	}

	private void insertViewChangeMessage(ViewChangeMessage message) {
		String query = "INSERT INTO ViewChanges VALUES (?, ?, ?, ?, ?, ?)";
		try (var pstmt = getPreparedStatement(query)) {
			pstmt.setInt(1, message.getNewViewNum());
			pstmt.setInt(2, message.getLastCheckpointNum());
			pstmt.setInt(3, message.getReplicaNum());
			pstmt.setString(4, serToString(message.getCheckPointMessages()));
			pstmt.setString(5, serToString(message.getMessageList()));
			pstmt.setString(6, serToString(message));

			pstmt.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			e.printStackTrace();
		}
	}

	private void insertNewViewMessage(NewViewMessage message) {
		String query = "INSERT INTO NewViewMessages VALUES (?, ?)";
		try (var pstmt = getPreparedStatement(query)) {
			pstmt.setInt(1, message.getNewViewNum());
			pstmt.setString(2, serToString(message));

			pstmt.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() == CONSTRAINT_ERROR)
				return;
			e.printStackTrace();
		}
	}

	void insertMessage(int sequenceNumber, ReplyMessage message) {
		insertReplyMessage(sequenceNumber, message);
	}

	private void insertReplyMessage(int seqNum, ReplyMessage message) {
		String query = "INSERT INTO Executed VALUES (?, ?)";
		try (var pstmt = getPreparedStatement(query)) {
			pstmt.setInt(1, seqNum);
			String data = serToString(message);
			pstmt.setString(2, data);
			pstmt.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean findMessage(Message message) {
		try {
			if (message instanceof RequestMessage) {
				return findRequestMessage((RequestMessage) message);
			} else if (message instanceof PreprepareMessage) {
				return findPreprepareMessage((PreprepareMessage) message);
			} else if (message instanceof PrepareMessage) {
				return findPrepareMessage((PrepareMessage) message);
			} else if (message instanceof CommitMessage) {
				return findCommitMessage((CommitMessage) message);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return false;
	}

	private boolean findRequestMessage(RequestMessage message) throws SQLException {
		String baseQuery = "SELECT COUNT(*) FROM Requests R WHERE R.client = ? AND R.timestamp = ? AND R.operation = ?";
		PreparedStatement pstatement = conn.prepareStatement(baseQuery);

		String data = serToString(message.getClientInfo());
		pstatement.setString(1, data);
		pstatement.setLong(2, message.getTime());
		String data1 = serToString(message.getOperation());
		pstatement.setString(3, data1);

		ResultSet ret = pstatement.executeQuery();
		if (ret.next())
			return ret.getInt(1) == 1;
		else
			throw new SQLException("Failed to find the message in the DB");
	}

	private boolean findPreprepareMessage(PreprepareMessage message) throws SQLException {
		String baseQuery = "SELECT COUNT(*) FROM Preprepares P WHERE P.viewNum = ? AND P.seqNum = ? AND P.digest = ?";
		PreparedStatement pstatement = conn.prepareStatement(baseQuery);

		pstatement.setInt(1, message.getViewNum());
		pstatement.setInt(2, message.getSeqNum());
		pstatement.setString(3, message.getDigest());

		ResultSet ret = pstatement.executeQuery();
		if (ret.next())
			return ret.getInt(1) == 1;
		else
			throw new SQLException("Failed to find the message in the DB");
	}

	private boolean findPrepareMessage(PrepareMessage message) throws SQLException {
		String baseQuery = "SELECT COUNT(*) FROM Prepares P WHERE P.viewNum = ? AND P.seqNum = ? AND P.digest = ? AND P.replica = ?";
		PreparedStatement pstatement = conn.prepareStatement(baseQuery);

		pstatement.setInt(1, message.getViewNum());
		pstatement.setInt(2, message.getSeqNum());
		pstatement.setString(3, message.getDigest());
		pstatement.setInt(4, message.getReplicaNum());

		ResultSet ret = pstatement.executeQuery();
		if (ret.next())
			return ret.getInt(1) == 1;
		else
			throw new SQLException("Failed to find the message in the DB");
	}

	private boolean findCommitMessage(CommitMessage message) throws SQLException {
		String baseQuery = "SELECT COUNT(*) FROM Commits C WHERE C.viewNum = ? AND C.seqNum = ? AND C.digest = ? AND C.replica = ?";
		PreparedStatement pstatement = conn.prepareStatement(baseQuery);

		pstatement.setInt(1, message.getViewNum());
		pstatement.setInt(2, message.getSeqNum());
		pstatement.setString(3, message.getDigest());
		pstatement.setInt(4, message.getReplicaNum());

		ResultSet ret = pstatement.executeQuery();
		if (ret.next())
			return ret.getInt(1) == 1;
		else
			throw new SQLException("Failed to find the message in the DB");
	}
}
