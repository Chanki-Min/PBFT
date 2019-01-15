package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Instant;

class SampleOperation extends Operation {

    SampleOperation(InetSocketAddress clientInfo, long timestamp) {
        super(clientInfo, timestamp);
    }

    @Override
    Result execute() {
        return null;
    }
}

class LoggerTest {

    @Test
    void createTables() {
        Logger logger = new Logger();
    }

    @Test
    void insertMessage() throws NoSuchAlgorithmException {
        Logger logger = new Logger();

        KeyPair keyPair = Util.generateKeyPair();
        PrivateKey privateKey = keyPair.getPrivate();
        PublicKey publicKey = keyPair.getPublic();

        Operation sampleOperation = new SampleOperation(
                new InetSocketAddress("0.0.0.0", 0),
                Instant.now().getEpochSecond());

        RequestMessage requestMessage = new RequestMessage(privateKey, sampleOperation);
        PreprepareMessage preprepareMessage = new PreprepareMessage(privateKey, 0, 0, sampleOperation);
        PrepareMessage prepareMessage = new PrepareMessage(privateKey, 0, 0, "hi", 0);
        CommitMessage commitMessage = new CommitMessage(privateKey, 0, 0, "hi", 0);

        logger.insertMessage(requestMessage);
        logger.insertMessage(preprepareMessage);
        logger.insertMessage(prepareMessage);
        logger.insertMessage(commitMessage);

    }

    @Test
    void findMessage() {
    }
}