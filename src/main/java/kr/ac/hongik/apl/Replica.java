package kr.ac.hongik.apl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Replica extends Connector implements Primary, Backup {
    static final String path = "src/main/resources/replica.properties";

    static final double timeout = 1.;
    int primary = 0;



    public Replica(Properties prop){
        super(prop);
    }

    public void broadcastRequest(Message msg) {

    }

    public void broadcastPrePrepare(Message message) {

    }

    public void broadcastPrepare(Message message) {

    }

    public void broadcastCommit(Message message) {

    }

    public void broadcastViewChange(Message message) {

    }

    public void broadcastNewView(Message message) {

    }

    public void broadcastCheckpoint(Message message) {

    }

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        FileInputStream fis = new FileInputStream(path);
        properties.load(new java.io.BufferedInputStream(fis));

        Replica replica = new Replica(properties);
    }
}
