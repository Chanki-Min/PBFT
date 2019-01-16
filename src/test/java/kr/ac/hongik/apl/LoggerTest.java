package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.sql.SQLException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        logger.deleteDBFile();
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

        logger.deleteDBFile();

    }

    @Test
    void findMessage() throws NoSuchAlgorithmException, SQLException {
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

        assertTrue(logger.findMessage(commitMessage));
        assertTrue(logger.findMessage(prepareMessage));
        assertTrue(logger.findMessage(preprepareMessage));
        assertTrue(logger.findMessage(requestMessage));

        RequestMessage requestMessage2 = new RequestMessage(privateKey, sampleOperation);
        PreprepareMessage preprepareMessage2 = new PreprepareMessage(privateKey, 1, 1, sampleOperation);
        PrepareMessage prepareMessage2 = new PrepareMessage(privateKey, 1, 1, "hello", 1);
        CommitMessage commitMessage2 = new CommitMessage(privateKey, 1, 1, "hello", 1);

        assertFalse(logger.findMessage(requestMessage2));
        assertFalse(logger.findMessage(preprepareMessage2));
        assertFalse(logger.findMessage(prepareMessage2));
        assertFalse(logger.findMessage(commitMessage2));

        logger.deleteDBFile();
    }
}
