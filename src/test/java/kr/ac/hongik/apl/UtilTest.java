package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.security.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UtilTest {
    @Test
    void digitalSignatureTest() throws NoSuchAlgorithmException, SignatureException, InvalidKeyException, NoSuchProviderException {
        String sampleData = "Hello, world!";
        KeyPair pair = Util.generateKeyPair();
        PrivateKey privateKey = pair.getPrivate();
        PublicKey publicKey = pair.getPublic();

        byte[] signature = Util.sign(privateKey, sampleData);

        assertTrue(Util.verify(publicKey, sampleData, signature));

        Message sample2 = new PrepareMessage(privateKey, 0, 0, "hihihi", 0);
        signature = Util.sign(privateKey, sample2);
        assertTrue(Util.verify(publicKey, sample2, signature));
    }

    @Test
    void serializationTest() throws NoSuchAlgorithmException {
        KeyPair keyPair = Util.generateKeyPair();
        PrivateKey privateKey = keyPair.getPrivate();

        Message expected = new PrepareMessage(privateKey, 0, 0, "hi", 0);

        byte[] ser = Util.serialize(expected);
        assertEquals(expected, Util.deserialize(ser));

        expected = new CommitMessage(privateKey, 1, 1, "hello", 1);
        ser = Util.serialize(expected);
        assertEquals(expected, Util.deserialize(ser));
    }

}