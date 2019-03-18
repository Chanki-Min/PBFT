package kr.ac.hongik.apl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static kr.ac.hongik.apl.RequestMessage.makeRequestMsg;

class ValidationTest {
	@Test
	void validationTest() throws IOException {
		/****** Create some blocks to test ******/
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		String artHash = "art1";

		Client client = new Client(prop);
		Management management = new Management(client.getPublicKey(), prop, artHash, "Alice", "Bob", 100, -1L);
		client.request(makeRequestMsg(client.getPrivateKey(), management));
		Object[] tmp = (Object[]) client.getReply();
		String root = (String) tmp[tmp.length - 1];
		System.out.println(root);

		//colllector 실행 -> 키 받기

		Collector collector = new Collector(client.getPublicKey(), root);
		client.request(makeRequestMsg(client.getPrivateKey(), collector));
		Object t = client.getReply();
		System.err.println(t.getClass().getName());
		List<Object> prePieces = (List<Object>) t;

		//toMap 이용해서 키 조합
		Map<Integer, byte[]> pieces = Util.toMap(prePieces, (Integer) tmp[0], (byte[]) tmp[1]);

		//validation
		Validation validation = new Validation(client.getPublicKey(), root, pieces, artHash);
		client.request(makeRequestMsg(client.getPrivateKey(), validation));

		Assertions.assertTrue((Boolean) client.getReply());
	}
}