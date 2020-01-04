package kr.ac.hongik.apl.Message;

import kr.ac.hongik.apl.Messages.CommitMessage;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;

import static kr.ac.hongik.apl.Util.generateKeyPair;

class CommitMessageTest {
	@Test
	void test() throws NoSuchAlgorithmException {
		System.out.println("CommitMessage Class Unit Test Start");
		KeyPair keyPair = generateKeyPair();
		CommitMessage commitMessage = CommitMessage.makeCommitMsg(keyPair.getPrivate(), 0, 0, "digest", 0);
		System.out.println("CommitMessage Class Unit Test Success");
	}
}