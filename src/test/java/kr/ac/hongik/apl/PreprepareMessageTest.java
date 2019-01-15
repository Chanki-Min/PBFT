package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;

import static kr.ac.hongik.apl.Util.generateKeyPair;

class PreprepareOperation extends Operation {

    PreprepareOperation(InetSocketAddress clientInfo, long timestamp) {
        super(clientInfo, timestamp);
    }

    @Override
    Result execute() {
        return null;
    }
}

class PreprepareMessageTest {
    @Test
    void test() throws NoSuchAlgorithmException {
        System.out.println("PreprepareMessage Class Unit Test Start");
        KeyPair keyPair = generateKeyPair();
        InetSocketAddress clientInfo = new InetSocketAddress("127.0.0.1", 3000);
        long timestamp = System.currentTimeMillis() / 1000;
        Operation operation = new PreprepareOperation(clientInfo, timestamp);
        PreprepareMessage preprepareMessage = new PreprepareMessage(keyPair.getPrivate(), 0, 0, operation);
        System.out.println("PreprepareMessage Class Unit Test Success");
    }
}