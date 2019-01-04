package kr.ac.hongik.apl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Client extends Connector {
    static final String path = "src/main/resources/replica.properties";


    public Client(Properties prop){
        super(prop);

    }

    private void request(Message msg){

    }

    Result reply(){

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
