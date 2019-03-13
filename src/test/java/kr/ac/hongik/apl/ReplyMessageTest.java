package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Instant;

import static kr.ac.hongik.apl.Util.generateKeyPair;

class ReplyResult implements Serializable {
    ReplyResult(PublicKey clientInfo, long timestamp) {
    }

}

class ReplyMessageTest {
    @Test
    void test() throws NoSuchAlgorithmException {
        System.out.println("ReplyMessage Class Unit Test Start");
        KeyPair keyPair = generateKeyPair();
        PublicKey clientInfo = keyPair.getPublic();
        long timestamp = Instant.now().getEpochSecond();
        Object result = new ReplyResult(clientInfo, timestamp);
        PrivateKey privateKey = keyPair.getPrivate();
        ReplyMessage replyMessage = new ReplyMessage(privateKey, 0, 0, clientInfo, 0, result);
        System.out.println("ReplyMessage Class Unit Test Success");
    }
}