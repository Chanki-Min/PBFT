package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.security.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

class UtilTest {
    @Test
    void keyTest() throws NoSuchAlgorithmException, SignatureException, InvalidKeyException, NoSuchProviderException {
        Result result = new Result();
        KeyPair pair = Util.generateKeyPair();
        PrivateKey priv = pair.getPrivate();
        PublicKey pub = pair.getPublic();

        byte[] sign = Util.sign(priv, result);

        assertTrue(Util.verify(pub, result, sign));
    }

}