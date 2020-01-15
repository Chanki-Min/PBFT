package kr.ac.hongik.apl.Operation;

import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.GetBlockChainListOperation;
import kr.ac.hongik.apl.Operations.Operation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class GetBlockChainListOperationTest {
	@Test
	public void getListTest() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);

		Operation operation = new GetBlockChainListOperation(client.getPublicKey());
		RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), operation);

		client.request(requestMessage);
		List<String> result = (List<String>) client.getReply();
	}
}
