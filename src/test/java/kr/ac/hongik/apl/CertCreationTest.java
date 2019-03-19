package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

class CertCreationTest {

	@Test
	void serializeTest() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		var replicas = Util.parseProperties(prop);
		Client client = new Client(prop);
		Map<Integer, byte[]> m = new HashMap<>();
		m.put(1, "hi".getBytes());
		CertCreation certCreation = new CertCreation(client.getPublicKey(), m);

		var tmp = Util.serialize(certCreation);

		var actual = Util.deserialize(tmp);

		Assertions.assertEquals(certCreation, actual);
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

		CertCreation expected = new CertCreation(client.getPublicKey(), m);

		var signed = Util.sign(client.getPrivateKey(), expected);

		var tmp = Util.serialize(expected);

		var actual = Util.deserialize(tmp);

		Assertions.assertTrue(Util.verify(client.getPublicKey(), actual, signed));

	}

}