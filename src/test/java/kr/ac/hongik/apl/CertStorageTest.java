package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

class CertStorageTest {

	@Test
	void serializeTest() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		var replicas = Util.parseProperties(prop);
		Client client = new Client(prop);
		Map<Integer, byte[]> m = new HashMap<>();
		m.put(1, "hi".getBytes());
		CertStorage certStorage = new CertStorage(client.getPublicKey(), m);

		var tmp = Util.serialize(certStorage);

		var actual = Util.deserialize(tmp);

		Assertions.assertEquals(certStorage, actual);
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

		CertStorage expected = new CertStorage(client.getPublicKey(), m);

		var signed = Util.sign(client.getPrivateKey(), expected);

		var tmp = Util.serialize(expected);

		var actual = Util.deserialize(tmp);

		Assertions.assertTrue(Util.verify(client.getPublicKey(), actual, signed));

	}

}