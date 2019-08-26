package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.GreetingOperation;
import kr.ac.hongik.apl.Operations.Operation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.Thread.sleep;


public class RunnableTest {
    @Test
    void requestTest() throws IOException {
        InputStream in = getClass().getResourceAsStream("/replica.properties");
        Properties prop = new Properties();
        prop.load(in);

        Client client = new Client(prop);
        Operation op = new GreetingOperation(client.getPublicKey());
		RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
        System.err.println("Client: Request");
        client.request(requestMessage);
        System.err.println("Client: try to get reply");
        var ret = client.getReply();

        Assertions.assertEquals("Hello, World!", ret.toString());
    }

    @Test
    void countlessRequest() throws IOException, InterruptedException {
        // request countless RequestMessages
        InputStream in = getClass().getResourceAsStream("/replica.properties");
        Properties prop = new Properties();
        prop.load(in);

        int maxClientNum = 20;
        List<Thread> clientThreadList = new ArrayList<>(maxClientNum);
        for (int i = 0; i < maxClientNum; i++) {
            Thread thread = new Thread(new CountlessClientTest(prop, i));
            clientThreadList.add(thread);
        }

        for (var i: clientThreadList) {
            i.start();
        }

        for (var i: clientThreadList) {
            i.join();
        }
    }

    @Test
    void oneClientManyOperations() throws IOException, InterruptedException {
        //run only 3 Replicas. Expected to make agreement only with 3 Replicas
        InputStream in = getClass().getResourceAsStream("/replica.properties");
        Properties prop = new Properties();
        prop.load(in);

        Client client = new Client(prop);
        System.err.println("Client: Request");
        Integer repeatTime = 5;
        for (int i = 0; i < repeatTime; i++) {
            Operation op = new GreetingOperation(client.getPublicKey());
			RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
            client.request(requestMessage);
            sleep(1000);
        }
        System.err.println("Client: try to get reply");
        for (int i = 0; i < repeatTime; i++) {
            var ret = client.getReply();
            System.err.println(ret.toString());
            Assertions.assertEquals("Hello, World!", ret.toString());
        }


    }

}
