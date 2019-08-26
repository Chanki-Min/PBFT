package kr.ac.hongik.apl.Blockchain;

import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.ES.EsJsonParser;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Operations.SQLSearchOperation;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Properties;

public class SqlSearchOperationTest {

	@Test
	public void SQLSearchOperationTest() throws IOException, NoSuchFieldException, EsRestClient.EsSSLException {
		String HttpProtocol = "GET";
		String query = "SELECT MAX(block_number)  from block_chain WHERE block_number = 20 AND entry_number = 0";

		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		Client client = new Client(prop);

		Operation sqlOp = new SQLSearchOperation(client.getPublicKey(), HttpProtocol, query);
		RequestMessage message = RequestMessage.makeRequestMsg(client.getPrivateKey(), sqlOp);

		long startTime = System.currentTimeMillis();
		client.request(message);
		String pbftResultBody = (String) client.getReply();

		System.err.println("Search by PBFT with SQL query finish with time : " + (System.currentTimeMillis() - startTime)
				+ "ms. trying to search directly from Es...");

		String lowLevelQuery = "{ \"query\" : \"" + query + "\" }";
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		Request request = new Request(HttpProtocol, "_sql/");
		request.addParameter("format", "json");
		request.setEntity(new NStringEntity(lowLevelQuery, ContentType.APPLICATION_JSON));

		Response response = esRestClient.getClient().getLowLevelClient().performRequest(request);
		RequestLine requestLine = response.getRequestLine();
		HttpHost host = response.getHost();
		int statusCode = response.getStatusLine().getStatusCode();
		Header[] headers = response.getHeaders();
		String directResponseBody = EntityUtils.toString(response.getEntity());
		esRestClient.disConnectToEs();

		Assertions.assertTrue(pbftResultBody.equals(directResponseBody));

		System.err.println("Assersion OK, printing pbftResultBody...");
		XContentParser parser = XContentFactory.xContent(XContentType.JSON)
				.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, pbftResultBody.getBytes());
		parser.close();
		XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint().copyCurrentStructure(parser);
		System.err.println(Strings.toString(builder));

		System.err.println("converting pbftResultBody to LinkedHashMap....");
		EsJsonParser esJsonParser = new EsJsonParser();
		LinkedHashMap resultMap = esJsonParser.sqlResponseStringToLinkedMap(pbftResultBody);
		resultMap.keySet().stream()
				.forEachOrdered(k -> System.err.print("[" + k + ", " + resultMap.get(k) + "], "));
	}

	@Test
	public void SqlQueryByLowClientTest() throws NoSuchFieldException, IOException {
		try {
			String query = "SELECT block_number, entry_number, start_time from block_chain where block_number = 1 AND entry_number = 0";
			query = "SELECT * from block_chain";
			query = sqlQueryGenerator(query);
			EsRestClient esRestClient = new EsRestClient();
			esRestClient.connectToEs();

			Request request = new Request("GET", "_sql/");
			request.addParameter("pretty", "true");
			request.addParameter("format", "json");
			request.setEntity(new NStringEntity(query, ContentType.APPLICATION_JSON));

			Response response = esRestClient.getClient().getLowLevelClient().performRequest(request);
			RequestLine requestLine = response.getRequestLine();
			HttpHost host = response.getHost();
			int statusCode = response.getStatusLine().getStatusCode();
			Header[] headers = response.getHeaders();
			String responseBody = EntityUtils.toString(response.getEntity());
			esRestClient.disConnectToEs();

			XContentParser parser = XContentFactory.xContent(XContentType.JSON)
					.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, responseBody.getBytes());
			parser.close();
			XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint().copyCurrentStructure(parser);
			System.err.println(Strings.toString(builder));

			EsJsonParser esJsonParser = new EsJsonParser();
			LinkedHashMap<Integer, LinkedHashMap> resultMap = esJsonParser.sqlResponseStringToLinkedMap(responseBody);
			resultMap.keySet().stream()
					.forEachOrdered(k -> System.err.println(k + " : [" + k + ", " + resultMap.get(k) + "], "));
		} catch (ResponseException | EsRestClient.EsSSLException e) {
			System.err.println(e.getMessage());
		}
	}

	private String sqlQueryGenerator(String query) {
		StringBuilder builder = new StringBuilder();
		return builder.append("{ \"query\" : \"").append(query).append("\" }").toString();
	}
}