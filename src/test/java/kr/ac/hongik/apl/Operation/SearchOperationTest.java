package kr.ac.hongik.apl.Operation;


import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Operations.SearchOperation;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SearchOperationTest {
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
		masterMap.put( "hostName", "223.194.70.105");
		masterMap.put( "port", "19192");
		masterMap.put( "hostScheme", "https");

		esRestClientConfigs.put("masterHostInfo", List.of(masterMap));
	}

	@Test
	public void searchOperationTest() throws Exception{
		String HttpProtocol = "GET";
		String endpoint = "_cat/nodes";
		Map<String, String> paramMap = new HashMap<>();
		paramMap.put("v", "true");
		paramMap.put("format", "json");
		String body = "";

		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);

		Operation op = new SearchOperation(client.getPublicKey(), esRestClientConfigs,
				HttpProtocol, endpoint, paramMap, body);

		RequestMessage searchMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
		client.request(searchMsg);
		Map reply = (Map) client.getReply();
	}

	@Test
	public void reflection() throws NoSuchFieldException, EsRestClient.EsSSLException, IOException, IllegalAccessException {
		String HttpProtocol = "GET";
		String endpoint = "_cat/nodes";
		Map<String, String> paramMap = new HashMap<>();
		paramMap.put("v", "true");
		paramMap.put("format", "json");
		String body = "";

		EsRestClient esRestClient = new EsRestClient(esRestClientConfigs);
		esRestClient.connectToEs();

		Request request = new Request(HttpProtocol, endpoint);
		paramMap.entrySet().stream().
				forEach(e -> request.addParameter(e.getKey(), e.getValue()));
		request.setEntity(new NStringEntity(body, org.apache.http.entity.ContentType.APPLICATION_JSON));
		Response response = esRestClient.getClient().getLowLevelClient().performRequest(request);

		Field field = response.getClass().getDeclaredField("response");
		field.setAccessible(true);
		BasicHttpResponse httpResponse = (BasicHttpResponse) field.get(response);
	}
}

