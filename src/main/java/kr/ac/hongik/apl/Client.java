package kr.ac.hongik.apl;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.Properties;

public class Client extends Connector {
    private static final String path = "src/main/resources/replica.properties";


    public Client(Properties prop){
        super(prop);    //make socket to every replica
    }

    //Empty method.
    @Override
    protected void acceptOp(SelectionKey key) { }

    private void request(Message msg){

    }

    void getReply(){
        /* TODO: 다수결 선택하기.
         * Result result = (Result) receive(); // replica로부터 reply 받는 방식
         */
    }

    private static Properties readProperties() throws IOException {
        Properties properties = new Properties();
        FileInputStream fis = new FileInputStream(path);
        properties.load(new java.io.BufferedInputStream(fis));

        return properties;
    }

    public static void main(String[] args) throws IOException {
        Client client = new Client(readProperties());
    }
}
