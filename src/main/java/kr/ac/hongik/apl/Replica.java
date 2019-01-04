package kr.ac.hongik.apl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Replica extends Connector implements Primary, Backup {
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
        String path = "samplecfg.txt";
        Properties properties = new Properties();
        properties.load(new FileInputStream(path));

        Replica replica = new Replica(properties);
    }
}
