package kr.ac.hongik.apl.Blockchain;

import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.GetBlockVerificationLogOperation;
import kr.ac.hongik.apl.Operations.Operation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class GetBlockVerificationLogOperationTest {
	@Test
	public void GetBilkVerificationOpTest() throws IOException{
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);

		Operation getVerifyLogOp = new GetBlockVerificationLogOperation(client.getPublicKey());
		RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), getVerifyLogOp);
		client.request(requestMessage);
		List<List<String>> result = (List<List<String>>) client.getReply();
		for(var lst : result){
			System.out.print("[ ");
			lst.stream().forEachOrdered(ent -> System.out.print(ent+", "));
			System.out.println(" ]");
		}
		System.err.println("Total size :"+result.size());
	}
}
