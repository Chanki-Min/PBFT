package kr.ac.hongik.apl.Blockchain;

import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.ES.EsJsonParser;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Operations.SearchOperation;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class SearchOperationTest {

	@Test
	public void searchOperationTest() throws IOException, NoSuchFieldException, EsRestClient.EsSSLException {
		String queryPath = "/EsSearchQuery/Query.json";
		String[] indices = {"block_chain"};

		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		Client client = new Client(prop);

		EsJsonParser parser = new EsJsonParser();
		parser.setFilePath(queryPath);

		Map queryMap = parser.jsonFileToMap();
		Operation searchOp = new SearchOperation(client.getPublicKey(), queryMap, indices);
		RequestMessage message = RequestMessage.makeRequestMsg(client.getPrivateKey(), searchOp);

		long startTime = System.currentTimeMillis();
		client.request(message);
		List<String> resultString = (List<String>) client.getReply();

		System.err.println("search end, resultSize :" + resultString.size() + " took :" + (System.currentTimeMillis() - startTime) + "ms");

		System.err.println("searchOperationTest : PBFT searchOp end, search to ES directly...");

		List<SearchHits> finalResult = new ArrayList();
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		QueryBuilder queryBuilder = QueryBuilders.wrapperQuery(Strings.toString(XContentFactory.jsonBuilder().map(queryMap)));

		SearchRequest searchRequest = new SearchRequest().indices(indices);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(queryBuilder);
		builder.size(10000);
		searchRequest.source(builder);
		startTime = System.currentTimeMillis();

		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		searchRequest.scroll(scroll);
		SearchResponse response = esRestClient.getClient().search(searchRequest, RequestOptions.DEFAULT);
		String scrollID = response.getScrollId();
		SearchHits hits = response.getHits();

		finalResult.add(hits);

		while (hits.getHits() != null && hits.getHits().length > 0) {
			SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollID);
			scrollRequest.scroll(scroll);
			response = esRestClient.getClient().scroll(scrollRequest, RequestOptions.DEFAULT);

			scrollID = response.getScrollId();
			hits = response.getHits();
			finalResult.add(hits);
		}
		esRestClient.disConnectToEs();
		List<String> ser = finalResult.stream().map(x -> Strings.toString(x)).collect(Collectors.toList());
		ser.remove(ser.size() - 1);

		System.err.println("Direct search end with size : " + finalResult.size() + " TIME :" + (System.currentTimeMillis() - startTime));

		System.err.println("result of PBFT search & Direct search SAME? : " + resultString.equals(ser));
		Assertions.assertTrue(resultString.equals(ser));
	}

	@Test
	public void searchByParsedMapTest() throws NoSuchFieldException, IOException, EsRestClient.EsSSLException {
		String queryPath = "/EsSearchQuery/Query.json";
		String indexName = "block_chain";
		List<SearchHits> finalResult = new ArrayList();
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		EsJsonParser parser = new EsJsonParser();
		parser.setFilePath(queryPath);
		Map queryMap = parser.jsonFileToMap();

		QueryBuilder queryBuilder = QueryBuilders.wrapperQuery(Strings.toString(XContentFactory.jsonBuilder().map(queryMap)));

		SearchRequest searchRequest = new SearchRequest().indices(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(queryBuilder);
		builder.size(10000);
		searchRequest.source(builder);
		long startTime = System.currentTimeMillis();

		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		searchRequest.scroll(scroll);
		SearchResponse response = esRestClient.getClient().search(searchRequest, RequestOptions.DEFAULT);
		String scrollID = response.getScrollId();
		SearchHits hits = response.getHits();

		finalResult.add(hits);

		while (hits.getHits() != null && hits.getHits().length > 0) {
			SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollID);
			scrollRequest.scroll(scroll);
			response = esRestClient.getClient().scroll(scrollRequest, RequestOptions.DEFAULT);

			scrollID = response.getScrollId();
			hits = response.getHits();
			finalResult.add(hits);
		}
		System.err.println("size : " + finalResult.size());
		System.err.println("passed TIME :" + (System.currentTimeMillis() - startTime));
		esRestClient.disConnectToEs();

		List<String> ser = finalResult.stream().map(x -> Strings.toString(x)).collect(Collectors.toList());
	}
}
