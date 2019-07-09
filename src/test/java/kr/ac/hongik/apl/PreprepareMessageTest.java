package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.PreprepareMessage;
import kr.ac.hongik.apl.Operations.Operation;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;

import static kr.ac.hongik.apl.Util.generateKeyPair;

class PreprepareOperation extends Operation {

    PreprepareOperation(PublicKey clientInfo, long timestamp) {
        super(clientInfo);
    }

    @Override
    public Object execute(Object obj) {
        return null;
    }
}

class PreprepareMessageTest {
    @Test
    void test() throws NoSuchAlgorithmException {
        System.out.println("PreprepareMessage Class Unit Test Start");
        KeyPair keyPair = generateKeyPair();
        PublicKey clientInfo = keyPair.getPublic();
        long timestamp = System.currentTimeMillis() / 1000;
        Operation operation = new PreprepareOperation(clientInfo, timestamp);
		PreprepareMessage preprepareMessage = PreprepareMessage.makePrePrepareMsg(keyPair.getPrivate(), 0, 0, operation);
        System.out.println("PreprepareMessage Class Unit Test Success");
    }
}