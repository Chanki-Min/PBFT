package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.Greeting;
import kr.ac.hongik.apl.Operations.GreetingOperation;
import kr.ac.hongik.apl.Operations.Operation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ClientTest {
	@Test
	public void closeConnectionTest() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);
		Replica.msgDebugger.info("Client Connection established");
		while (true) {
			Operation op = new GreetingOperation(client.getPublicKey());
			RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
			client.request(requestMessage);

		Greeting reply = (Greeting) client.getReply();
		Replica.msgDebugger.info(String.format("Got reply : %s", reply.greeting));
		}
		//client.close();
		//Replica.msgDebugger.info("Client Connection closed");
	}

	@Test
	public void heartbeatTest() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);
		while(true) {
			long init = System.currentTimeMillis();
			int i=0;
			client.checkSocketChannel();
			Replica.detailDebugger.trace(String.format("check end with time %d", System.currentTimeMillis() - init));
		}
	}
}
