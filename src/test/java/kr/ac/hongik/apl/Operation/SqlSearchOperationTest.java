package kr.ac.hongik.apl.Operation;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.ES.EsJsonParser;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Operations.SQLSearchOperation;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class SqlSearchOperationTest {
	private Map<String, Object> esRestClientConfigs;
	private ObjectMapper objectMapper = new ObjectMapper()
			.enable(JsonParser.Feature.ALLOW_COMMENTS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

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
	public void SQLSearchOperationTest() throws IOException {
		int fetchSize = 1000;
		String HttpProtocol = "GET";
		String query = "SELECT * from test_block_chain";

		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		Client client = new Client(prop);

		Operation sqlOp = new SQLSearchOperation(client.getPublicKey(), esRestClientConfigs,HttpProtocol, query, fetchSize, false);
		RequestMessage message = RequestMessage.makeRequestMsg(client.getPrivateKey(), sqlOp);

		client.request(message);
		long startTime = System.currentTimeMillis();
		String pbftResultBody = (String) client.getReply();
		System.err.println("got Response with time took: " + (System.currentTimeMillis() - startTime));
		System.err.println("converting pbftResultBody to LinkedHashMap....");
		EsJsonParser esJsonParser = new EsJsonParser();
		LinkedHashMap resultMap = esJsonParser.sqlResponseStringToLinkedMap(pbftResultBody);
		int i=0;
	}

	@Test
	public void SqlQueryByLowClientTest() throws NoSuchFieldException, IOException {
		try {
			String query = "SELECT block_number, entry_number, start_time from block_chain where block_number = 1 AND entry_number = 0";
			query = "SELECT * from test_block_chain";
			query = sqlQueryGenerator("query", query);
			EsRestClient esRestClient = new EsRestClient(esRestClientConfigs);
			esRestClient.connectToEs();

			Response response = getResponse(query);
			String responseBody = EntityUtils.toString(response.getEntity());
			while (getCursorId(responseBody) != null) {
				String cursor = getCursorId(responseBody);
				cursor = sqlQueryGenerator("cursor", cursor);
				response = getResponse(cursor);

				String cursorBody = EntityUtils.toString(response.getEntity());
				responseBody = concatBody(responseBody, cursorBody);
				int i = 0;
			}
			esRestClient.disConnectToEs();
			EsJsonParser esJsonParser = new EsJsonParser();
			LinkedHashMap<Integer, LinkedHashMap> resultMap = esJsonParser.sqlResponseStringToLinkedMap(responseBody);
		} catch (ResponseException | EsRestClient.EsSSLException e) {
			System.err.println(e.getMessage());
		}
	}

	private Response getResponse(String query) throws IOException, ResponseException, NoSuchFieldException, EsRestClient.EsSSLException {
		EsRestClient esRestClient = new EsRestClient(esRestClientConfigs);
		try {
			esRestClient.connectToEs();
			Request request = new Request("GET", "_sql/");
			request.addParameter("format", "json");
			request.setEntity(new NStringEntity(query, ContentType.APPLICATION_JSON));
			return esRestClient.getClient().getLowLevelClient().performRequest(request);
		} finally {
			esRestClient.disConnectToEs();
		}
	}

	private String getCursorId(String body) throws JsonProcessingException {
		Map map = objectMapper.readValue(body, Map.class);
		if (map.containsKey("cursor")) {
			return (String) map.get("cursor");
		} else {
			return null;
		}
	}

	private String concatBody(String to, String from) throws IOException {
		Map toMap = objectMapper.readValue(to, Map.class);
		Map fromMap = objectMapper.readValue(from, Map.class);

		if (toMap.containsKey("rows") && fromMap.containsKey("rows")) {
			((List) toMap.get("rows")).addAll((List) fromMap.get("rows"));
		} else {
			throw new IOException("to, from has wrong format");
		}
		if (fromMap.containsKey("cursor")) {
			toMap.put("cursor", fromMap.get("cursor"));
		} else {
			toMap.remove("cursor");
		}
		return objectMapper.writeValueAsString(toMap);
	}

	private String sqlQueryGenerator(String key, String query) {
		StringBuilder builder = new StringBuilder();
		return builder.append("{ \"").append(key).append("\" : \"").append(query).append("\"}").toString();
	}
}