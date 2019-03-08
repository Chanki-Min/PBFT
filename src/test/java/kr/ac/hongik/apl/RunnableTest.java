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

import static java.lang.Thread.sleep;
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
        System.err.println("Client: Request");
        client.request(requestMessage);
        System.err.println("Client: try to get reply");
        var ret =  client.getReply();

        Assertions.assertEquals("Hello, World!", ret.toString());
    }

    @Test
    void countlessRequest() throws IOException, InterruptedException {
        // request countless RequestMessages
        InputStream in = getClass().getResourceAsStream("/replica.properties");
        Properties prop = new Properties();
        prop.load(in);

        int maxClientNum = 2;
        List<Thread> clientThreadList = new ArrayList<>(maxClientNum);
        for(int i = 0; i < maxClientNum; i++){
            Thread thread = new Thread(new CountlessClientTest(prop, i));
            clientThreadList.add(thread);
        }

        for(var i : clientThreadList){
            i.start();
        }

        for(var i : clientThreadList){
           i.join();
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
