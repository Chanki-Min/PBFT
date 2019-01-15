package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;

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
        long timestamp = System.currentTimeMillis() / 1000;
        Result result = new ReplyResult(clientInfo, timestamp);
        ReplyMessage replyMessage = new ReplyMessage(keyPair.getPrivate(), 0, 0, "127.0.0.1", 0, 0, result);
        System.out.println("ReplyMessage Class Unit Test Success");
    }
}