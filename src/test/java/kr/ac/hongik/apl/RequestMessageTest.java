package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.time.Instant;

import static kr.ac.hongik.apl.Util.generateKeyPair;


class RequestOperation extends Operation {

    RequestOperation(PublicKey clientInfo, long timestamp) {
        super(clientInfo, timestamp);
    }

    @Override
    Object execute() {
        return null;
    }
}

class RequestMessageTest {
    @Test
    void test() throws NoSuchAlgorithmException {
        System.out.println("RequestMessage Class Unit Test Start");
        KeyPair keyPair = generateKeyPair();
        PublicKey clientInfo = keyPair.getPublic();
        long timestamp = Instant.now().getEpochSecond();
        Operation operation = new RequestOperation(clientInfo, timestamp);
        RequestMessage requestMessage = new RequestMessage(keyPair.getPrivate(), operation);
        System.out.println("RequestMessage Class Unit Test Success");
    }
}