package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.sql.SQLException;

import static kr.ac.hongik.apl.Util.generateKeyPair;

class PrepareMessageTest {
    @Test
    void test() throws NoSuchAlgorithmException {
        System.out.println("PrepareMessage Class Unit Test Start");
        KeyPair keyPair = generateKeyPair();
        PrepareMessage prepareMessage = new PrepareMessage(keyPair.getPrivate(), 0, 0, "digest", 0);
        System.out.println("PrepareMessage Class Unit Test Success");
    }

    @Test
    void fromCommitMessage() throws SQLException {
        KeyPair keyPair = Util.generateKeyPair();
        PrivateKey privateKey = keyPair.getPrivate();
        PrepareMessage expected = new PrepareMessage(privateKey, 1, 1, "hi", 1);
        CommitMessage commitMessage = new CommitMessage(privateKey, expected.getViewNum(),
                expected.getSeqNum(), expected.getDigest(), expected.getReplicaNum());

        Logger logger = new Logger();
        logger.insertMessage(expected);

        PrepareMessage actual = PrepareMessage.fromCommitMessage(commitMessage);
        Assertions.assertTrue(logger.findMessage(actual));
        logger.deleteDBFile();
    }
}