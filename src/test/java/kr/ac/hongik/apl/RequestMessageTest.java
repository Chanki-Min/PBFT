package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.Operation;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.PublicKey;
import java.time.Instant;

import static kr.ac.hongik.apl.Util.generateKeyPair;


class RequestOperation extends Operation {

	RequestOperation(PublicKey clientInfo, long timestamp) {
		super(clientInfo);
	}

	@Override
	public Object execute(Object obj) {
		return null;
	}
}

class RequestMessageTest {
	@Test
	void test() {
		System.out.println("RequestMessage Class Unit Test Start");
		KeyPair keyPair = generateKeyPair();
		PublicKey clientInfo = keyPair.getPublic();
		long timestamp = Instant.now().getEpochSecond();
		Operation operation = new RequestOperation(clientInfo, timestamp);
		RequestMessage requestMessage = RequestMessage.makeRequestMsg(keyPair.getPrivate(), operation);
		System.out.println("RequestMessage Class Unit Test Success");
	}
}