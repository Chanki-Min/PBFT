package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static kr.ac.hongik.apl.RequestMessage.makeRequestMsg;

class BlockCreationTest {

	@Test
	void serializeTest() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		var replicas = Util.parseProperties(prop);
		Client client = new Client(prop);
		Map<Integer, byte[]> m = new HashMap<>();
		m.put(1, "hi".getBytes());
		BlockPayload expected = new BlockPayload("art1", "Alice", "Bob", 100, -1L);

		var tmp = Util.serialize(expected);

		var actual = Util.deserialize(tmp);

		Assertions.assertEquals(expected, actual);
	}

	@Test
	void signTest() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		var replicas = Util.parseProperties(prop);
		Client client = new Client(prop);
		Map<Integer, byte[]> m = new HashMap<>();
		m.put(1, "hi".getBytes());
		BlockPayload expected = new BlockPayload("art1", "Alice", "Bob", 100, -1L);
		var signed = Util.sign(client.getPrivateKey(), expected);

		var tmp = Util.serialize(expected);

		var actual = Util.deserialize(tmp);

		Assertions.assertTrue(Util.verify(client.getPublicKey(), actual, signed));

	}

	@Test
	void creationTest() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		List<InetSocketAddress> replicas = Util.parseProperties(prop);
		BlockPayload payload1 = new BlockPayload("art1", "Alice", "Bob", 100, -1L);
		BlockPayload payload2 = new BlockPayload("art1", "Bob", "Charlie", 110, -1L);
		BlockPayload payload3 = new BlockPayload("art1", "Charlie", "Alice", 120, -1L);

		BlockPayload[] list = {payload1, payload2, payload3};

		Client client = new Client(prop);
		for (var payload : list) {
			BlockCreation blockCreation = new BlockCreation(client.getPublicKey(), payload);
			System.err.println(Util.hash(blockCreation));
			var msg = makeRequestMsg(client.getPrivateKey(), blockCreation);
			if (!msg.verify(client.getPublicKey())) throw new AssertionError();
			client.request(msg);
			String header = (String) client.getReply();

			System.err.println("Header: " + header);

			int n = replicas.size() + 2;
			int f = replicas.size() / 3;
			Map<Integer, byte[]> pieces = Util.split(header, n, f);

			//TODO: cert에서 걸렸다....;
			CertCreation certCreation = new CertCreation(client.getPublicKey(), pieces);
			System.err.println(certCreation.toString());
			client = new Client(prop);
			var msg1 = makeRequestMsg(client.getPrivateKey(), certCreation);
			if (!msg1.verify(client.getPublicKey())) throw new AssertionError();
			client.request(msg1);
			System.out.println("requested");
			List<Object> roots = (List<Object>) client.getReply();
			String root = (String) roots.get(0);
			roots.stream().map(x -> (String) x).forEach(System.err::println);
		}
	}

	@Test
	void test() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		List<InetSocketAddress> replicas = Util.parseProperties(prop);
		BlockPayload payload1 = new BlockPayload("art1", "Alice", "Bob", 100, -1L);
		BlockPayload payload2 = new BlockPayload("art1", "Bob", "Charlie", 110, -1L);
		BlockPayload payload3 = new BlockPayload("art1", "Charlie", "Alice", 120, -1L);

		BlockPayload[] list = {payload1, payload2, payload3};

		Client client = new Client(prop);
		String header = "hihihihihi";
		int n = replicas.size() + 2;
		int f = replicas.size() / 3;
		Map<Integer, byte[]> pieces = Util.split(header, n, f);

		//TODO: cert에서 걸렸다....;
		CertCreation certCreation = new CertCreation(client.getPublicKey(), pieces);
		client = new Client(prop);
		client.request(makeRequestMsg(client.getPrivateKey(), certCreation));
		System.out.println("requested");
		List<Object> roots = (List<Object>) client.getReply();
		String root = (String) roots.get(0);
		roots.stream().map(x -> (String) x).forEach(System.err::println);
	}
}