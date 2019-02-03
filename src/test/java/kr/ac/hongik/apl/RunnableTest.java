package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.PublicKey;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

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
    void test() throws IOException {
        try {
            InputStream in = getClass().getResourceAsStream("/replica.properties");
            Properties prop = new Properties();
            prop.load(in);
            List<Replica> replicas = IntStream.range(0, 4).mapToObj(i -> {
                String addr = prop.getProperty("replica" + i);
                var addrs = addr.split(":");
                var ret = new Replica(prop, addrs[0], Integer.parseInt(addrs[1]));
                return ret;
            }).collect(toList());

            replicas.parallelStream().forEach(Connector::connect);

            replicas.stream().map(x -> {
                var thread = new Thread(() -> x.start());
                return thread;
            }).forEach(Thread::start);
        } finally {
            File file = new File("src/main/resources/");
            Arrays.stream(file.listFiles())
                    .filter(x -> x.getName().contains(".db"))
                    .forEach(File::delete);
        }
    }

    @Test
    void requestTest() throws IOException {
        InputStream in = getClass().getResourceAsStream("/replica.properties");
        Properties prop = new Properties();
        prop.load(in);


        Client client = new Client(prop);
        Operation op = new GreetingOperation(client.getPublicKey());
        RequestMessage requestMessage = new RequestMessage(client.getPrivateKey(), op);

        client.request(requestMessage);
        Greeting ret = (Greeting) client.getReply();


        Assertions.assertEquals("Hello, World!", ret.toString());

    }
}
