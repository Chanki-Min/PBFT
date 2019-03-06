package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.security.PublicKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static kr.ac.hongik.apl.Util.verify;


public class RunnableTest {

    @Test
    void requestTest() throws IOException {
        InputStream in = getClass().getResourceAsStream("/replica.properties");
        Properties prop = new Properties();
        prop.load(in);


        Client client = new Client(prop);
        Operation op = new GreetingOperation(client.getPublicKey());
        RequestMessage requestMessage = new RequestMessage(client.getPrivateKey(), op);
        boolean check = verify(client.getPublicKey(), requestMessage.getOperation(), requestMessage.getSignature());
        if(check){
            System.err.println("Before broadcast PrePrepare, Verify");
        }
        else{
            System.err.println("Before bradcast PrePrepare, Not Verify");
        }
        System.err.println("Client: Request");
        client.request(requestMessage);
        System.err.println("Client: try to get reply");
        var ret =  client.getReply();


        Assertions.assertEquals("Hello, World!", ret.toString());
    }

    @Test
    void countlessRequest() throws IOException {
        // request countless RequestMessages
        InputStream in = getClass().getResourceAsStream("/replica.properties");
        Properties prop = new Properties();
        prop.load(in);


        Client client = new Client(prop);
        for(int i = 0; i < 10; i++){
            Operation op = new GreetingOperation(client.getPublicKey());
            RequestMessage requestMessage = new RequestMessage(client.getPrivateKey(), op);
            System.err.println("Client: Request " + i);
            client.request(requestMessage);
        }
        System.err.println("Client: try to get reply");
        while(true) {
            var ret = client.getReply();
            Assertions.assertEquals("Hello, World!", ret.toString());
        }

    }

    @Test
    void runThreeReplicas(){
        //run only 3 Replicas. Expected to make agreement only with 3 Replicas

    }

    /*
     * TODO: Run each Replica on the independent server with own IP.
     */
}
