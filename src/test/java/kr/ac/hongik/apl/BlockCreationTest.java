package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
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
		/****** Create some blocks to test ******/
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		var replicas = Util.parseProperties(prop);
		String artHash = "art1";

		Client client = new Client(prop);
		BlockCreation blockCreation = new BlockCreation(client.getPublicKey(), artHash, "Alice", "Bob", 100, -1L);
		client.request(makeRequestMsg(client.getPrivateKey(), blockCreation));
		String header = (String) client.getReply();

		int n = replicas.size() + 2;
		int f = replicas.size() / 3;
		Map<Integer, byte[]> pieces = Util.split(header, n, f);

		client = new Client(prop);
		CertStorage certStorage = new CertStorage(client.getPublicKey(), pieces);
		client.request(makeRequestMsg(client.getPrivateKey(), certStorage));
		List<Object> roots = (List<Object>) client.getReply();

		Assertions.assertTrue(roots.stream().map(x -> (String) x).distinct().count() == 1);


		BlockCreation blockCreation1 = new BlockCreation(client.getPublicKey(), artHash, "Bob", "Charlie", 200, -1L);
		client.request(makeRequestMsg(client.getPrivateKey(), blockCreation1));
		String header1 = (String) client.getReply();

		Map<Integer, byte[]> pieces1 = Util.split(header, n, f);

		client = new Client(prop);
		CertStorage certStorage1 = new CertStorage(client.getPublicKey(), pieces1);
		client.request(makeRequestMsg(client.getPrivateKey(), certStorage1));
		List<Object> roots1 = (List<Object>) client.getReply();

		Assertions.assertTrue(roots1.stream().map(x -> (String) x).distinct().count() == 1);
	}

}