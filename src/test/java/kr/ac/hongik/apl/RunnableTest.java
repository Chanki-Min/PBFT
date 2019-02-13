package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.security.PublicKey;
import java.time.Instant;
import java.util.Properties;

class Greeting implements Result {
    public String greeting;

    public Greeting(String greeting) {
        this.greeting = greeting;
    }

    @Override
    public String toString() {
        return this.greeting;
    }
}

class GreetingOperation extends Operation {

    protected GreetingOperation(PublicKey clientInfo) {
        super(clientInfo, Instant.now().getEpochSecond());
    }

    @Override
    Result execute() {
        return new Greeting("Hello, World!");
    }
}

public class RunnableTest {

    @Test
    void requestTest() throws IOException {
        InputStream in = getClass().getResourceAsStream("/loopback.properties");
        Properties prop = new Properties();
        prop.load(in);


        Client client = new Client(prop);
        Operation op = new GreetingOperation(client.getPublicKey());
        RequestMessage requestMessage = new RequestMessage(client.getPrivateKey(), op);

        System.err.println("Client: Request");
        client.request(requestMessage);
        System.err.println("Client: try to get reply");
        Greeting ret = (Greeting) client.getReply();


        Assertions.assertEquals("Hello, World!", ret.toString());
    }
}
