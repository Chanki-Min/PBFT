package kr.ac.hongik.apl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
     * Requests:    (^client, ^timestamp, ^message). Insert this triple after its signature is validated. Primary only.
     * Preprepares: (^viewNum, ^seqNum, ^digest, fullRequest)
     * Prepares:    (^viewNum, ^seqNum, ^digest, ^i)
     * Commits:     (^viewNum, ^seqNum, ^digest, ^i)
     * Checkpoints: (^seqNum, ^stateDigest, ^i)
     */
    private void createTables(){
        String[] querys = {
                "CREATE TABLE Requests (client BLOB,timestamp DATE,message BLOB, PRIMARY KEY(client, timestamp, message))",
                "CREATE TABLE Preprepares (viewNum INT, seqNum INT, digest TEXT, message BLOB, PRIMARY KEY(viewNum, seqNum, digest))",
                "CREATE TABLE Prepares (viewNum INT, seqNum INT, digest TEXT, replica INT, PRIMARY KEY(viewNum, seqNum, digest, replica))",
                "CREATE TABLE Commits (viewNum INT, seqNum INT, digest TEXT, replica INT, PRIMARY KEY(seqNum, replica))",
                "CREATE TABLE Checkpoints (seqNum INT, stateDigest TEXT, replica INT, PRIMARY KEY(seqNum, stateDigest, replica))",
        };
            for(String query : querys){
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

    void insertMessage(Message message){
    }

    boolean searchMessage(Message message){

        return true;
    }
}
