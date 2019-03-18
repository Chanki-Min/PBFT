package kr.ac.hongik.apl;

import com.codahale.shamir.Scheme;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.security.*;
import java.util.HashMap;
import java.util.Map;

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

    @Test
    void secretSharingTest() throws InterruptedException {
        Scheme scheme = new Scheme(new SecureRandom(), 5, 3);
        String expected = "hi";
        Map<Integer, byte[]> dt = scheme.split(expected.getBytes());

        Thread.sleep(100);

        Scheme newScheme = new Scheme(new SecureRandom(), 5, 3);
        Map<Integer, byte[]> dt2 = newScheme.split(expected.getBytes());
        Assertions.assertEquals(expected, new String(newScheme.join(dt)));

        dt.values().forEach(System.out::print);
        System.out.println();
        dt2.values().forEach(System.out::print);

        Assertions.assertEquals(expected, new String(scheme.join(dt2)));

        var dt3 = new HashMap<Integer, byte[]>();

        dt3.put(1, dt2.get(1));
        dt3.put(2, dt2.get(2));
        dt3.put(3, dt.get(3));
        dt3.put(4, dt.get(4));

        Assertions.assertNotEquals(expected, new String(scheme.join(dt3)));
        /*
        결과: Secret Sharing은 SecureRandom과 무관하게 작동한다.
        SecureRandom이 stateful하지 않으니, split메소드도 stateful하지 않는다.
        그럼에도 불구하고, 임의의 SecureRandom에 대해 k개 이상의 한 scheme에서 생성된 key-value 쌍이 있으면 정상적으로 join된다.
        또한, 다른 scheme으로 생성된 pieces간에 섞어 사용하는 것은 금지된다.
         */
    }

}