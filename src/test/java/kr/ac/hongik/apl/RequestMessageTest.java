package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;

import static kr.ac.hongik.apl.Util.generateKeyPair;


class RequestOperation extends Operation {

    RequestOperation(InetSocketAddress clientInfo, long timestamp) {
        super(clientInfo, timestamp);
    }

    @Override
    Result execute() {
        return null;
    }
}

class RequestMessageTest {
    @Test
    void test() throws NoSuchAlgorithmException {
        System.out.println("RequestMessage Class Unit Test Start");
        KeyPair keyPair = generateKeyPair();
        InetSocketAddress clientInfo = new InetSocketAddress("127.0.0.1", 3000);
        long timestamp = Instant.now().getEpochSecond();
        Operation operation = new RequestOperation(clientInfo, timestamp);
        RequestMessage requestMessage = new RequestMessage(keyPair.getPrivate(), operation);
        System.out.println("RequestMessage Class Unit Test Success");
    }
}