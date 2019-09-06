package kr.ac.hongik.apl.Operation;

import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.BlockVerificationOperation;
import kr.ac.hongik.apl.Operations.Operation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class BlockVerificationOperationTest {
	@Test
	public void verifyBlockOpTest() throws IOException {
		List<String> indices = Arrays.asList("car_log", "user_log");
		int blockNumber = 1;

		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);

		Operation verifyBlockOp = new BlockVerificationOperation(client.getPublicKey(), blockNumber, indices);
		RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), verifyBlockOp);
		long send = System.currentTimeMillis();
		client.request(insertRequestMsg);
		List result = (List) client.getReply();

		System.err.printf("got reply from PBFT with time %dms\n", (System.currentTimeMillis() - send));
		int y = 0;
	}
}
