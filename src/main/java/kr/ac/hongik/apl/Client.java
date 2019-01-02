package kr.ac.hongik.apl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Client extends Connector {


    public Client(Properties prop){
        super(prop);

    }
    void request(Message msg){

    }
    Result reply(){

    }

    static void main(String[] args) throws IOException {
        String path = "samplecfg.txt";
        Properties properties = new Properties()
        properties.load(new FileInputStream(path));

        Client client = new Client(properties);
    }
}
