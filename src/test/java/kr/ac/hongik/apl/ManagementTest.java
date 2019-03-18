package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import static kr.ac.hongik.apl.RequestMessage.makeRequestMsg;

class ManagementTest {

	@Test
	void creationTest() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		BlockPayload payload1 = new BlockPayload("art1", "Alice", "Bob", 100, -1L);
		BlockPayload payload2 = new BlockPayload("art1", "Bob", "Charlie", 110, -1L);
		BlockPayload payload3 = new BlockPayload("art1", "Charlie", "Alice", 120, -1L);

		BlockPayload[] list = {payload1, payload2, payload3};

		Client client = new Client(prop);
		for (var payload : list) {
			Management management = new Management(client.getPublicKey(), prop, payload);
			client.request(makeRequestMsg(client.getPrivateKey(), management));
			Object[] ret = (Object[]) client.getReply();
			Arrays.stream(ret).forEach(System.out::println);
		}

	}

}