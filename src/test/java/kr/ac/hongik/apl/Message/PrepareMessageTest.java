package kr.ac.hongik.apl.Message;

import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Messages.CommitMessage;
import kr.ac.hongik.apl.Messages.PrepareMessage;
import kr.ac.hongik.apl.Util;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.PrivateKey;

import static kr.ac.hongik.apl.Util.generateKeyPair;

class PrepareMessageTest {
	String dbFileName = "test";

	@Test
	void test() {
		System.out.println("PrepareMessage Class Unit Test Start");
		KeyPair keyPair = generateKeyPair();
		PrepareMessage prepareMessage = PrepareMessage.makePrepareMsg(keyPair.getPrivate(), 0, 0, "digest", 0);
		System.out.println("PrepareMessage Class Unit Test Success");
	}

	@Test
	void fromCommitMessage() {
		KeyPair keyPair = Util.generateKeyPair();
		PrivateKey privateKey = keyPair.getPrivate();
		PrepareMessage expected = PrepareMessage.makePrepareMsg(privateKey, 1, 1, "hi", 1);
		CommitMessage commitMessage = CommitMessage.makeCommitMsg(privateKey, expected.getViewNum(),
				expected.getSeqNum(), expected.getDigest(), expected.getReplicaNum());

		Logger logger = new Logger();
		logger.insertMessage(expected);

		PrepareMessage actual = PrepareMessage.fromCommitMessage(commitMessage);
		Assertions.assertTrue(logger.findMessage(actual));

	}
}
