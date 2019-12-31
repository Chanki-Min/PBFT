package kr.ac.hongik.apl.Operation;

import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.GetHeaderOperation;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Operations.OperationExecutionException;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class GetHeaderOperationTest {
	@Test
	public void getHeaderOpTest() throws IOException {
		int blockNumber = 0;

		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);

		Operation getHeaderOp = new GetHeaderOperation(client.getPublicKey(), blockNumber);
		RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), getHeaderOp);
		client.request(insertRequestMsg);

		Object reply = client.getReply();
		if(reply instanceof OperationExecutionException) {
			OperationExecutionException exception = (OperationExecutionException) reply;
			exception.getMessage();
		} else {
			Triple<Integer, String, String> result = (Triple<Integer, String, String>) client.getReply();
			Assertions.assertNotNull(result);
			System.err.println(result.toString());
		}
	}
}
