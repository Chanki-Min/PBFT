package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;

import static kr.ac.hongik.apl.Util.generateKeyPair;

class PrepareMessageTest {
    @Test
    void test() throws NoSuchAlgorithmException {
        System.out.println("PrepareMessage Class Unit Test Start");
        KeyPair keyPair = generateKeyPair();
        PrepareMessage prepareMessage = new PrepareMessage(keyPair.getPrivate(), 0, 0, "digest", 0);
        System.out.println("PrepareMessage Class Unit Test Success");
    }
}