package kr.ac.hongik.apl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.Properties;
import java.io.InputStream;

public class Client extends Connector {
    static String path = "src/main/java/kr/ac/hongik/apl/sampleCfg.properties";


    public Client(Properties prop){
        super(prop);

    }
     private void request(Message msg){

    }
    /*
    Result reply(){

    }
    */

    private  static Properties readProperties() throws IOException {
        Properties properties = new Properties();
        FileInputStream fis = new FileInputStream(path);
        properties.load(new java.io.BufferedInputStream(fis));

        return properties;
    }

    public void showEndpoints(){
        for(int i = 0; i < endpoints.size(); i++){
            System.out.print("["+ i + "]\t" + "ip : " + endpoints.get(i).ip);
            System.out.print("\t" + "port : " + endpoints.get(i).port);

        }
    }

    public static void main(String[] args) throws IOException {
        Client client = new Client(readProperties());
        client.showEndpoints();
    }
}
