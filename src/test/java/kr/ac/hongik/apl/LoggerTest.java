package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SampleOperation extends Operation {

    SampleOperation(PublicKey clientInfo, long timestamp) {
        super(clientInfo);
    }

    @Override
    public Object execute(Object obj) {
        return null;
    }
}

class LoggerTest {

    @Test
    void createTables() {
        Logger logger = new Logger();

    }

    @Test
    void insertPreprepareMessage(){
        Logger logger = new Logger();

        try {
            KeyPair keyPair = Util.generateKeyPair();
            PrivateKey privateKey = keyPair.getPrivate();
            PublicKey publicKey = keyPair.getPublic();

            Operation sampleOperation = new SampleOperation(
                    publicKey,
                    Instant.now().getEpochSecond());

            PreprepareMessage preprepareMessage = PreprepareMessage.makePrePrepareMsg(privateKey, 0, 0, sampleOperation);

            logger.insertMessage(preprepareMessage);
            logger.insertMessage(preprepareMessage);

        } finally {

        }
    }

    @Test
	void insertMessage() {
        Logger logger = new Logger();

        try {
            KeyPair keyPair = Util.generateKeyPair();
            PrivateKey privateKey = keyPair.getPrivate();
            PublicKey publicKey = keyPair.getPublic();

            Operation sampleOperation = new SampleOperation(
                    publicKey,
                    Instant.now().getEpochSecond());

            RequestMessage requestMessage = RequestMessage.makeRequestMsg(privateKey, sampleOperation);
            PreprepareMessage preprepareMessage = PreprepareMessage.makePrePrepareMsg(privateKey, 0, 0, sampleOperation);
            PrepareMessage prepareMessage = PrepareMessage.makePrepareMsg(privateKey, 0, 0, "hi", 0);
            CommitMessage commitMessage = CommitMessage.makeCommitMsg(privateKey, 0, 0, "hi", 0);

            logger.insertMessage(requestMessage);
            logger.insertMessage(preprepareMessage);
            logger.insertMessage(prepareMessage);
            logger.insertMessage(commitMessage);
        } finally {

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

            RequestMessage requestMessage = RequestMessage.makeRequestMsg(privateKey, sampleOperation);

            logger.insertMessage(requestMessage);

            assertTrue(logger.findMessage(requestMessage));

            Operation sampleOperation2 = new SampleOperation(
                    publicKey,
                    Instant.now().getEpochSecond()
            );
            RequestMessage requestMessage2 = RequestMessage.makeRequestMsg(privateKey, sampleOperation2);

            assertFalse(logger.findMessage(requestMessage2));
        } finally {

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

            PreprepareMessage preprepareMessage = PreprepareMessage.makePrePrepareMsg(privateKey, 0, 0, sampleOperation);

            logger.insertMessage(preprepareMessage);

            assertTrue(logger.findMessage(preprepareMessage));

            PreprepareMessage preprepareMessage2 = PreprepareMessage.makePrePrepareMsg(privateKey, 1, 1, sampleOperation);

            assertFalse(logger.findMessage(preprepareMessage2));
        } finally {

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

            PrepareMessage prepareMessage = PrepareMessage.makePrepareMsg(privateKey, 0, 0, "hi", 0);

            logger.insertMessage(prepareMessage);

            assertTrue(logger.findMessage(prepareMessage));

            PrepareMessage prepareMessage2 = PrepareMessage.makePrepareMsg(privateKey, 1, 1, "hello", 1);

            assertFalse(logger.findMessage(prepareMessage2));
        } finally {

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

            CommitMessage commitMessage = CommitMessage.makeCommitMsg(privateKey, 0, 0, "hi", 0);

            logger.insertMessage(commitMessage);

            assertTrue(logger.findMessage(commitMessage));

            CommitMessage commitMessage2 = CommitMessage.makeCommitMsg(privateKey, 1, 1, "hello", 1);

            assertFalse(logger.findMessage(commitMessage2));
        } finally {

        }
    }

    @Test
    public void testGarbageCollection() {
        try {
            InputStream in = getClass().getResourceAsStream("/replica.properties");

            Properties prop = new Properties();
            prop.load(in);

            Client client = new Client(prop);
            System.err.println("Client: Request");
            Integer repeatTime = 150;
            for (int i = 1; i <= repeatTime; i++) {
                Operation op = new CountMessages(client.getPublicKey());
                RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
                client.request(requestMessage);
                int[] ret = (int[]) client.getReply();
                System.err.printf("#%d: ", i);
                Arrays.stream(ret).forEach(x -> System.err.print(x + " "));
                System.err.println();
                Assertions.assertEquals(i % Replica.WATERMARK_UNIT, ret[3] % Replica.WATERMARK_UNIT);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
