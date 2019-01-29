package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.sql.SQLException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SampleOperation extends Operation {

    SampleOperation(PublicKey clientInfo, long timestamp) {
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

        try {
            KeyPair keyPair = Util.generateKeyPair();
            PrivateKey privateKey = keyPair.getPrivate();
            PublicKey publicKey = keyPair.getPublic();

            Operation sampleOperation = new SampleOperation(
                    publicKey,
                    Instant.now().getEpochSecond());

            RequestMessage requestMessage = new RequestMessage(privateKey, sampleOperation);
            PreprepareMessage preprepareMessage = new PreprepareMessage(privateKey, 0, 0, sampleOperation);
            PrepareMessage prepareMessage = new PrepareMessage(privateKey, 0, 0, "hi", 0);
            CommitMessage commitMessage = new CommitMessage(privateKey, 0, 0, "hi", 0);

            logger.insertMessage(requestMessage);
            logger.insertMessage(preprepareMessage);
            logger.insertMessage(prepareMessage);
            logger.insertMessage(commitMessage);
        } finally {
            logger.deleteDBFile();
        }
    }

    @Test
    void findRequestMessage() throws SQLException, NoSuchAlgorithmException {

        Logger logger = new Logger();
        try {
            KeyPair keyPair = Util.generateKeyPair();
            PrivateKey privateKey = keyPair.getPrivate();
            PublicKey publicKey = keyPair.getPublic();

            Operation sampleOperation = new SampleOperation(
                    publicKey,
                    Instant.now().getEpochSecond());

            RequestMessage requestMessage = new RequestMessage(privateKey, sampleOperation);

            logger.insertMessage(requestMessage);

            assertTrue(logger.findMessage(requestMessage));

            Operation sampleOperation2 = new SampleOperation(
                    publicKey,
                    Instant.now().getEpochSecond()
            );
            RequestMessage requestMessage2 = new RequestMessage(privateKey, sampleOperation2);

            assertFalse(logger.findMessage(requestMessage2));
        } finally {
            logger.deleteDBFile();
        }
    }

    @Test
    void findPreprepareMessage() throws NoSuchAlgorithmException, SQLException {

        Logger logger = new Logger();
        try {
            KeyPair keyPair = Util.generateKeyPair();
            PrivateKey privateKey = keyPair.getPrivate();
            PublicKey publicKey = keyPair.getPublic();

            Operation sampleOperation = new SampleOperation(
                    publicKey,
                    Instant.now().getEpochSecond());

            PreprepareMessage preprepareMessage = new PreprepareMessage(privateKey, 0, 0, sampleOperation);

            logger.insertMessage(preprepareMessage);

            assertTrue(logger.findMessage(preprepareMessage));

            PreprepareMessage preprepareMessage2 = new PreprepareMessage(privateKey, 1, 1, sampleOperation);

            assertFalse(logger.findMessage(preprepareMessage2));
        } finally {
            logger.deleteDBFile();
        }
    }

    @Test
    void findPrepareMessage() throws NoSuchAlgorithmException, SQLException {

        Logger logger = new Logger();
        try {
            KeyPair keyPair = Util.generateKeyPair();
            PrivateKey privateKey = keyPair.getPrivate();
            PublicKey publicKey = keyPair.getPublic();

            Operation sampleOperation = new SampleOperation(
                    publicKey,
                    Instant.now().getEpochSecond());

            PrepareMessage prepareMessage = new PrepareMessage(privateKey, 0, 0, "hi", 0);

            logger.insertMessage(prepareMessage);

            assertTrue(logger.findMessage(prepareMessage));

            PrepareMessage prepareMessage2 = new PrepareMessage(privateKey, 1, 1, "hello", 1);

            assertFalse(logger.findMessage(prepareMessage2));
        } finally {
            logger.deleteDBFile();
        }
    }

    @Test
    void findCommitMessage() throws NoSuchAlgorithmException, SQLException {

        Logger logger = new Logger();
        try {
            KeyPair keyPair = Util.generateKeyPair();
            PrivateKey privateKey = keyPair.getPrivate();
            PublicKey publicKey = keyPair.getPublic();

            Operation sampleOperation = new SampleOperation(
                    publicKey,
                    Instant.now().getEpochSecond());

            CommitMessage commitMessage = new CommitMessage(privateKey, 0, 0, "hi", 0);

            logger.insertMessage(commitMessage);

            assertTrue(logger.findMessage(commitMessage));

            CommitMessage commitMessage2 = new CommitMessage(privateKey, 1, 1, "hello", 1);

            assertFalse(logger.findMessage(commitMessage2));
        } finally {
            logger.deleteDBFile();
        }
    }
}
