package kr.ac.hongik.apl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.*;

import static kr.ac.hongik.apl.Util.serialize;

public class Logger {
    private Connection conn = null;


    public Logger() {
        String url = "jdbc:sqlite:src/main/resources/" + this.toString() + ".db";
        try {
            this.conn= DriverManager.getConnection(url);
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
     */
    private void createTables(){
        String[] queries = {
                "CREATE TABLE Requests (client BLOB,timestamp DATE,operation BLOB, PRIMARY KEY(client, timestamp, operation))",
                "CREATE TABLE Preprepares (viewNum INT, seqNum INT, digest TEXT, operation BLOB, PRIMARY KEY(viewNum, seqNum, digest))",
                "CREATE TABLE Prepares (viewNum INT, seqNum INT, digest TEXT, replica INT, PRIMARY KEY(viewNum, seqNum, digest, replica))",
                "CREATE TABLE Commits (viewNum INT, seqNum INT, digest TEXT, replica INT, PRIMARY KEY(seqNum, replica))",
                "CREATE TABLE Checkpoints (seqNum INT, stateDigest TEXT, replica INT, PRIMARY KEY(seqNum, stateDigest, replica))",
        };
            for(String query : queries){
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

    public void close(){
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertPreprepareMessage(PreprepareMessage message){
        String baseQuery = "INSERT INTO Preprepares VALUES ( ?, ?, ?, ? )";
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(baseQuery);

            preparedStatement.setInt(1, message.getViewNum());
            preparedStatement.setInt(2, message.getSeqNum());
            preparedStatement.setString(3, message.getDigest());
            byte[] bytes = serialize(message.getOperation());
            InputStream in = new ByteArrayInputStream(bytes);

            preparedStatement.setBlob(4, in);

            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void insertMessage(Message message){
        if(message instanceof PreprepareMessage){
            insertPreprepareMessage((PreprepareMessage) message);
        }

    }

    boolean findMessage(Message message) throws SQLException {
        if(message instanceof PreprepareMessage){
            return findPreprepareMessage((PreprepareMessage) message);
        }


        else
            return false;
    }

    private boolean findPreprepareMessage(PreprepareMessage message) throws SQLException {
       String baseQuery = "SELECT COUNT(*) FROM Prepares P WHERE P.viewNum = ? AND P.seqNum = ? AND P.digest = ?";
            PreparedStatement pstatement = conn.prepareStatement(baseQuery);

            pstatement.setInt(1, message.getViewNum());
            pstatement.setInt(2, message.getSeqNum());
            pstatement.setString(3, message.getDigest());

            ResultSet ret = pstatement.executeQuery();
            if(ret.next())
                return ret.getInt(1) > 0;
            else
                throw new SQLException("Failed to find the message in the DB");
    }
}
