package kr.ac.hongik.apl.Operation;

import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.BlockVerificationOperation;
import kr.ac.hongik.apl.Operations.Operation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class BlockVerificationOperationTest {
	private Map<String, Object> esRestClientConfigs;

	@BeforeEach
	public void makeConfig() {
		esRestClientConfigs = new HashMap<>();
		esRestClientConfigs.put("userName", "apl");
		esRestClientConfigs.put("passWord", "wowsan2015@!@#$");
		esRestClientConfigs.put("certPath", "/ES_Connection/esRestClient-cert.p12");
		esRestClientConfigs.put("certPassWord", "wowsan2015@!@#$");

		Map<String, Object> masterMap = new HashMap<>();
		masterMap.put( "name", "es01-master01");
		masterMap.put( "hostName", "223.194.70.111");
		masterMap.put( "port", "51192");
		masterMap.put( "hostScheme", "https");

		esRestClientConfigs.put("masterHostInfo", List.of(masterMap));
	}

	@Test
	public void verifyBlockOpTest() throws IOException {
		List<String> indices = Arrays.asList("car_log", "user_log");
		int blockNumber = 1;

		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);

		Operation verifyBlockOp = new BlockVerificationOperation(client.getPublicKey(), esRestClientConfigs, blockNumber, indices);
		RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), verifyBlockOp);
		long send = System.currentTimeMillis();
		client.request(insertRequestMsg);
		List result = (List) client.getReply();

		System.err.printf("got reply from PBFT with time %dms\n", (System.currentTimeMillis() - send));
		int y = 0;
	}
}
