package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.time.Instant;

import static kr.ac.hongik.apl.Util.generateKeyPair;

class ReplyResult implements Result {
    ReplyResult(InetSocketAddress clientInfo, long timestamp) {
    }

}

class ReplyMessageTest {
    @Test
    void test() throws NoSuchAlgorithmException {
        System.out.println("ReplyMessage Class Unit Test Start");
        KeyPair keyPair = generateKeyPair();
        InetSocketAddress clientInfo = new InetSocketAddress("127.0.0.1", 3000);
        long timestamp = Instant.now().getEpochSecond();
        Result result = new ReplyResult(clientInfo, timestamp);
        PrivateKey privateKey = keyPair.getPrivate();
        ReplyMessage replyMessage = new ReplyMessage(privateKey, 0, 0, clientInfo, 0, result);
        System.out.println("ReplyMessage Class Unit Test Success");
    }
}