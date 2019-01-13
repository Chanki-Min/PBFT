package kr.ac.hongik.apl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Logger {
    private Connection conn = null;

    /**
     * Schema Design
     * ^ symbol means it is a prime attribute.
     * Request: (^client, ^timestamp, ^message). Insert this triple after its signature is validated. Primary only.
     * Preprepare: (^viewNum, ^seqNum, ^digest, fullRequest)
     * Prepare: (^viewNum, ^seqNum, ^digest, ^i)
     * Commit: (^viewNum, ^seqNum, ^digest, ^i)
     * Checkpoint(^seqNum, ^stateDigest, ^i)
     */
    public Logger() {
        String url = "jdbc;sqlite:src/main/resources/" + this.toString() + ".db";
        try {
            this.conn= DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        String[] querys = {
                "CREATE TABLE Request (client BLOB,timestamp DATE,message BLOB, PRIMARY KEY(client, timestamp, message))",
                "CREATE TABLE Preprepare (viewNum INT, seqNum INT, digest TEXT, message BLOB, PRIMARY KEY(viewNum, seqNum, digest))",
                "CREATE TABLE Prepare (viewNum INT, seqNum INT, digest TEXT, replica INT, PRIMARY KEY(viewNum, seqNum, digest, replica))",
                "CREATE TABLE Commit (viewNum INT, seqNum INT, digest TEXT, replica INT, PRIMARY KEY(viewNum, seqNum, digest, replica))",
                "CREATE TABLE Checkpoint (seqNum INT, stateDigest TEXT, replica INT, PRIMARY KEY(seqNum, stateDigest, replica))",
        };
        try {
            for(String query : querys){
                PreparedStatement preparedStatement = conn.prepareStatement(query);
                preparedStatement.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void InsertMessage(Message message){
    }
}
