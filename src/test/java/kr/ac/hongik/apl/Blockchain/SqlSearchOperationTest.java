package kr.ac.hongik.apl.Blockchain;

import com.owlike.genson.Genson;
import com.owlike.genson.GensonBuilder;
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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SqlSearchOperationTest {

	@Test
	public void SQLSearchOperationTest() throws IOException, NoSuchFieldException, EsRestClient.EsSSLException {
		String HttpProtocol = "GET";
		String query = "SELECT * from block_chain where block_number = 1";

		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		Client client = new Client(prop);

		Operation sqlOp = new SQLSearchOperation(client.getPublicKey(), HttpProtocol, query);
		RequestMessage message = RequestMessage.makeRequestMsg(client.getPrivateKey(), sqlOp);

		client.request(message);
		long startTime = System.currentTimeMillis();
		String pbftResultBody = (String) client.getReply();
		System.err.println("got Response with time took: " + (System.currentTimeMillis() - startTime));
		System.err.println("converting pbftResultBody to LinkedHashMap....");
		EsJsonParser esJsonParser = new EsJsonParser();
		LinkedHashMap resultMap = esJsonParser.sqlResponseStringToLinkedMap(pbftResultBody);
	}

	@Test
	public void SqlQueryByLowClientTest() throws NoSuchFieldException, IOException {
		try {
			String query = "SELECT block_number, entry_number, start_time from block_chain where block_number = 1 AND entry_number = 0";
			query = "SELECT block_number, entry_number from block_chain ORDER BY block_number ASC, entry_number ASC";
			query = sqlQueryGenerator("query", query);
			EsRestClient esRestClient = new EsRestClient();
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
		EsRestClient esRestClient = new EsRestClient();
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

	private String getCursorId(String body) {
		Genson genson = new GensonBuilder().useClassMetadata(true).useRuntimeType(true).create();
		Map map = genson.deserialize(body, Map.class);
		if (map.containsKey("cursor")) {
			return (String) map.get("cursor");
		} else {
			return null;
		}
	}

	private String concatBody(String to, String from) throws IOException {
		Genson genson = new GensonBuilder().useClassMetadata(true).useRuntimeType(true).create();
		Map toMap = genson.deserialize(to, Map.class);
		Map fromMap = genson.deserialize(from, Map.class);

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
		return genson.serialize(toMap);
	}

	private String sqlQueryGenerator(String key, String query) {
		StringBuilder builder = new StringBuilder();
		return builder.append("{ \"").append(key).append("\" : \"").append(query).append("\"}").toString();
	}
}