package kr.ac.hongik.apl;

import java.io.File;
import java.sql.*;
import java.util.Arrays;

import static java.util.Base64.getEncoder;
import static kr.ac.hongik.apl.Util.serialize;

public class Logger {
    private Connection conn = null;


    public Logger() {
        String url = "jdbc:sqlite:src/main/resources/" + this.toString() + ".db";
        try {
            this.conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.createTables();
    }

    /**
     * Schema Design
     * ^ symbol means it is a prime attribute.
     * Requests:    (^client, ^timestamp, ^operation). Insert this triple after its signature is validated. Primary only.
     * Preprepares: (^viewNum, ^seqNum, ^digest, operation)
     * Prepares:    (^viewNum, ^seqNum, ^digest, ^i)
     * Commits:     (^viewNum, ^seqNum, ^digest, ^i)
     * Checkpoints: (^seqNum, ^stateDigest, ^i)
     *
     * Blob insertion and comparison won't work so We are going to use serialized Base64 encoding instead of Blob
     */
    private void createTables() {
        String[] queries = {
                "CREATE TABLE Requests (client TEXT,timestamp DATE,operation TEXT, PRIMARY KEY(client, timestamp, operation))",
                "CREATE TABLE Preprepares (viewNum INT, seqNum INT, digest TEXT, operation TEXT, PRIMARY KEY(viewNum, seqNum, digest))",
                "CREATE TABLE Prepares (viewNum INT, seqNum INT, digest TEXT, replica INT, PRIMARY KEY(viewNum, seqNum, digest, replica))",
                "CREATE TABLE Commits (viewNum INT, seqNum INT, digest TEXT, replica INT, PRIMARY KEY(seqNum, replica))",
                "CREATE TABLE Checkpoints (seqNum INT, stateDigest TEXT, replica INT, PRIMARY KEY(seqNum, stateDigest, replica))",
        };
        for (String query : queries) {
            try {
                PreparedStatement preparedStatement = conn.prepareStatement(query);
                preparedStatement.execute();
            } catch (SQLException e) {
                System.err.println(query);
                System.err.println(e.getSQLState());
                e.printStackTrace();
            }
        }
    }

    void close() {
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Close the database connection and delete entire database files
     */
    void deleteDBFile() {
        this.close();
        this.conn = null;
        File file = new File("src/main/resources/");
        Arrays.stream(file.listFiles())
                .filter(x -> x.getName().contains(this.toString()))
                .forEach(File::delete);
    }

    PreparedStatement getPreparedStatement(String baseQuery) throws SQLException {
        return conn.prepareStatement(baseQuery);
    }

    void insertMessage(Message message) {
        if (message instanceof RequestMessage) {
            insertRequestMessage((RequestMessage) message);
        } else if (message instanceof PreprepareMessage) {
            insertPreprepareMessage((PreprepareMessage) message);
        } else if (message instanceof PrepareMessage) {
            insertPrepareMessage((PrepareMessage) message);
        } else if (message instanceof CommitMessage) {
            insertCommitMessage((CommitMessage) message);
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
            e.printStackTrace();
        }
    }

    private void insertPrepareMessage(PrepareMessage message) {
        String baseQuery = "INSERT INTO Prepares VALUES ( ?, ?, ?, ? )";
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(baseQuery);

            preparedStatement.setInt(1, message.getViewNum());
            preparedStatement.setInt(2, message.getSeqNum());
            preparedStatement.setString(3, message.getDigest());
            preparedStatement.setInt(4, message.getReplicaNum());

            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertRequestMessage(RequestMessage message) {
        String baseQuery = "INSERT INTO Requests (client, timestamp, operation) VALUES ( ?, ?, ? )";
        try {
            PreparedStatement pstmt = conn.prepareStatement(baseQuery);

            String clientBase64 = getEncoder().encodeToString(serialize(message.getClientInfo()));
            pstmt.setString(1, clientBase64);
            pstmt.setLong(2, message.getTime());
            String operationBase64 = getEncoder().encodeToString(serialize(message.getOperation()));
            pstmt.setString(3, operationBase64);

            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertPreprepareMessage(PreprepareMessage message) {
        String baseQuery = "INSERT INTO Preprepares VALUES ( ?, ?, ?, ? )";
        try {
            PreparedStatement pstmt = conn.prepareStatement(baseQuery);

            pstmt.setInt(1, message.getViewNum());
            pstmt.setInt(2, message.getSeqNum());
            pstmt.setString(3, message.getDigest());
            String operationBase64 = getEncoder().encodeToString(serialize(message.getOperation()));
            pstmt.setString(4, operationBase64);

            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean findMessage(Message message) throws SQLException {
        if (message instanceof RequestMessage) {
            return findRequestMessage((RequestMessage) message);
        } else if (message instanceof PreprepareMessage) {
            return findPreprepareMessage((PreprepareMessage) message);
        } else if (message instanceof PrepareMessage) {
            return findPrepareMessage((PrepareMessage) message);
        } else if (message instanceof CommitMessage) {
            return findCommitMessage((CommitMessage) message);
        } else
            throw new SQLException("Message type is incompatible");
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

    private boolean findRequestMessage(RequestMessage message) throws SQLException {
        String baseQuery = "SELECT COUNT(*) FROM Requests R WHERE R.client = ? AND R.timestamp = ? AND R.operation = ?";
        PreparedStatement pstatement = conn.prepareStatement(baseQuery);

        String clientBase64 = getEncoder().encodeToString(serialize(message.getClientInfo()));
        pstatement.setString(1, clientBase64);
        pstatement.setLong(2, message.getTime());
        String operationBase64 = getEncoder().encodeToString(serialize(message.getOperation()));
        pstatement.setString(3, operationBase64);

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
}
