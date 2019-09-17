package kr.ac.hongik.apl.ES;


import kr.ac.hongik.apl.Util;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import javax.net.ssl.SSLContext;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.*;

import static java.lang.Thread.sleep;

public class ElasticSearchTest {
	private static EsRestClient esRestClient;

	@Test
	void httpsConnectionTest() throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, KeyManagementException {
		final CredentialsProvider credentialsProvider =
				new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY,
				new UsernamePasswordCredentials("elastic", "wowsan2015@!@#$"));

		String certPath = "/ES_Connection/esRestClient-cert.p12";

		KeyStore trustStore = KeyStore.getInstance("jks");
		InputStream is = this.getClass().getResourceAsStream(certPath);
		trustStore.load(is, new String("wowsan2015@!@#$").toCharArray());

		SSLContextBuilder sslBuilder = SSLContexts.custom()
				.loadTrustMaterial(trustStore, null);
		final SSLContext sslContext = sslBuilder.build();

		RestClientBuilder builder = RestClient.builder(
				new HttpHost("223.194.70.111", 51192, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
						return httpAsyncClientBuilder.setSSLContext(sslContext)
								.setDefaultCredentialsProvider(credentialsProvider);
					}
				});

		RestHighLevelClient restHighLevelClient = new RestHighLevelClient(builder);
		restHighLevelClient.close();
	}

	@Test
	void esConnectionTest() throws IOException {
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();
		} catch (NoSuchFieldException | EsRestClient.EsSSLException e) {
			e.printStackTrace();
		}
		boolean isConnected = false;

		try {
			isConnected = !esRestClient.getClusterInfo().isTimedOut();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("ClusterInfo Requset successful? : " + isConnected);
		try {
			esRestClient.disConnectToEs();
		} catch (IOException e) {
			e.printStackTrace();
		}

		esRestClient.disConnectToEs();
		Assertions.assertEquals(true, isConnected);
	}

	@Test
	void indexCreate_DeleteTest() {
		EsJsonParser parser = new EsJsonParser();
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();
			XContentBuilder mappingBuilder;
			XContentBuilder settingBuilder;
			parser.setFilePath("/ES_MappingAndSetting/Debug_test_mapping.json");
			mappingBuilder = parser.jsonFileToXContentBuilder(false);

			parser.setFilePath("/ES_MappingAndSetting/Setting.json");
			settingBuilder = parser.jsonFileToXContentBuilder(false);

			EsRestClient esRestClient = new EsRestClient();
			esRestClient.connectToEs();
			esRestClient.createIndex("test_block_chain", mappingBuilder, settingBuilder);

			Assertions.assertEquals(true, isIndexExists("test_block_chain"));
			System.out.println("Index Creation test Successful, deleting test-index...");
			esRestClient.deleteIndex("test_block_chain");
			Assertions.assertEquals(false, isIndexExists("test_block_chain"));
			esRestClient.disConnectToEs();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	void SingleInsertTest() throws IOException {
		String indexName = "test_block_chain9";
		int blockNumber = 0;
		int entrySize = 100;
		int sleepTime = 1000;
		int versionNumber = 1;
		boolean deleteIndexAfterFinish = false;
		EsJsonParser parser = new EsJsonParser();
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();
			XContentBuilder mappingBuilder;
			XContentBuilder settingBuilder;
			parser.setFilePath("/ES_MappingAndSetting/Debug_test_mapping.json");
			mappingBuilder = parser.jsonFileToXContentBuilder(false);

			parser.setFilePath("/ES_MappingAndSetting/Setting.json");
			settingBuilder = parser.jsonFileToXContentBuilder(false);

			esRestClient.createIndex(indexName, mappingBuilder, settingBuilder);

			parser.setFilePath("/ES_userData/Debug_test_data.json");
			List<Map<String, Object>> sampleUserData = new ArrayList<>();
			List<byte[]> encData = new ArrayList<>();

			String seed = "Hello World!";
			SecretKey key = Util.makeSymmetricKey(seed);

			for (int i = 0; i < entrySize; i++) {
				var map = parser.jsonFileToMap();
				map.put("start_time", String.valueOf(System.currentTimeMillis()));
				sampleUserData.add(map);
				encData.add(Util.encrypt(Util.serToString((Serializable) sampleUserData.get(i)).getBytes(), key));
			}
			for (int i = 0; i < entrySize; i++) {
				IndexRequest request = new IndexRequest(indexName);
				sampleUserData.get(i).put("encrypt_data", Base64.getEncoder().encodeToString(encData.get(i)));
				sampleUserData.get(i).put("block_number", String.valueOf(blockNumber));
				sampleUserData.get(i).put("entry_number", String.valueOf(i));
				request.source(sampleUserData.get(i));
				long time = System.currentTimeMillis();
				IndexResponse response = esRestClient.getClient().index(request, RequestOptions.DEFAULT);
				System.err.print(System.currentTimeMillis() - time + "ms ");
				System.err.println(response.status());
				System.err.println("index success " + i + "/" + entrySize);
			}

			Assertions.assertEquals(entrySize, getBlockEntrySize(indexName, blockNumber) - 1);
			if (deleteIndexAfterFinish) esRestClient.deleteIndex(indexName);
		} catch (EsRestClient.EsConcurrencyException | EsRestClient.EsException | NoSuchFieldException | EsRestClient.EsSSLException | Util.EncryptionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			throw new IOError(e);
		} finally {
			esRestClient.disConnectToEs();
		}
	}

	@Test
	public void bulkProcessorTest() throws IOException {
		String indexName = "test_block_chain101";
		int blockNumber = 1;
		int entrySize = 1000;
		int sleepTime = 1000;
		int versionNumber = 1;
		boolean deleteIndexAfterFinish = true;
		EsJsonParser parser = new EsJsonParser();
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();
			XContentBuilder mappingBuilder;
			XContentBuilder settingBuilder;
			parser.setFilePath("/ES_MappingAndSetting/Debug_test_mapping.json");
			mappingBuilder = parser.jsonFileToXContentBuilder(false);

			parser.setFilePath("/ES_MappingAndSetting/Setting.json");
			settingBuilder = parser.jsonFileToXContentBuilder(false);

			esRestClient.createIndex(indexName, mappingBuilder, settingBuilder);

			parser.setFilePath("/ES_userData/Debug_test_data.json");
			List<Map<String, Object>> sampleUserData = new ArrayList<>();
			List<byte[]> encData = new ArrayList<>();

			String seed = "Hello World!";
			SecretKey key = Util.makeSymmetricKey(seed);

			for (int i = 0; i < entrySize; i++) {
				var map = parser.jsonFileToMap();
				map.put("start_time", String.valueOf(System.currentTimeMillis()));
				sampleUserData.add(map);
				encData.add(Util.encrypt(Util.serToString((Serializable) sampleUserData.get(i)).getBytes(), key));
			}

			long time = System.currentTimeMillis();
			esRestClient.bulkInsertDocumentByProcessor(indexName, blockNumber, sampleUserData, encData, versionNumber, 10000, 10, ByteSizeUnit.MB, 5);
			System.err.println("time :" + (System.currentTimeMillis() - time));

			if (deleteIndexAfterFinish) esRestClient.deleteIndex(indexName);
		} catch (InterruptedException | EsRestClient.EsConcurrencyException | EsRestClient.EsException | NoSuchFieldException | EsRestClient.EsSSLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			throw new IOError(e);
		} catch (Util.EncryptionException e) {
			e.printStackTrace();
		} finally {
			esRestClient.disConnectToEs();
		}

	}

	@Test
	void getBlockDataPairTest() throws IOException {
		String indexName = "test_block_chain10ls" +
				"";
		int blockNumber = 0;
		int entrySize = 100;
		int versionNumber = 1;
		int sleepTime = 5000;
		boolean deleteIndexAfterFinish = false;
		EsJsonParser parser = new EsJsonParser();
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();
			XContentBuilder mappingBuilder;
			XContentBuilder settingBuilder;
			parser.setFilePath("/ES_MappingAndSetting/Debug_test_mapping.json");
			mappingBuilder = parser.jsonFileToXContentBuilder(false);

			parser.setFilePath("/ES_MappingAndSetting/Setting.json");
			settingBuilder = parser.jsonFileToXContentBuilder(false);

			String seed = "Hello World!";
			SecretKey key = Util.makeSymmetricKey(seed);

			esRestClient.createIndex(indexName, mappingBuilder, settingBuilder);

			parser.setFilePath("/ES_userData/Debug_test_data.json");
			List<Map<String, Object>> sampleUserData = new ArrayList<>();
			List<byte[]> encData = new ArrayList<>();

			for (int i = 0; i < entrySize; i++) {
				var map = parser.jsonFileToMap();
				map.put("start_time", String.valueOf(System.currentTimeMillis()));
				sampleUserData.add(map);
				encData.add(Util.encrypt(Util.serToString((Serializable) sampleUserData.get(i)).getBytes(), key));
			}
			System.err.println("Test data ready");

			long currTime = System.currentTimeMillis();
			esRestClient.bulkInsertDocumentByProcessor(
					indexName, 0, sampleUserData, encData, 1, 100, 10, ByteSizeUnit.MB, 5);
			System.err.println("entrySize :" + entrySize + " bulkInsertionTime :" + (System.currentTimeMillis() - currTime));

			sleep(sleepTime);

			currTime = System.currentTimeMillis();
			Pair<List<Map<String, Object>>, List<byte[]>> resultPair = esRestClient.getBlockDataPair(indexName, blockNumber);
			System.err.println("entrySize :" + entrySize + " getAllDataTime :" + (System.currentTimeMillis() - currTime));
			Assertions.assertTrue(isDataEquals(encData, resultPair.getRight()));
			esRestClient.disConnectToEs();
			if (deleteIndexAfterFinish) esRestClient.deleteIndex("test_block_chain");
		} catch (InterruptedException | EsRestClient.EsConcurrencyException | EsRestClient.EsException | NoSuchFieldException | EsRestClient.EsSSLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			throw new IOError(e);
		} catch (Util.EncryptionException e) {
			e.printStackTrace();
		}
	}

	@Test
	void concurrentBulkInsertTest() {
		int entrySize = 1000;
		int sleepTime = 1000;
		int maxThreadNum = 10;
		List<Thread> threadList = new ArrayList<>(maxThreadNum);
		String indexName = "test_block_chain";
		int blockNumber = 0;

		EsJsonParser parser = new EsJsonParser();
		parser.setFilePath("/ES_userData/Debug_test_data.json");
		List<Map<String, Object>> sampleUserData = new ArrayList<>();
		List<byte[]> encData = new ArrayList<>();

		for (int i = 0; i < entrySize; i++) {
			sampleUserData.add(parser.jsonFileToMap());
			encData.add(Util.serToString((Serializable) sampleUserData.get(i)).getBytes());
		}

		try {
			for (int i = 0; i < maxThreadNum; i++) {
				Thread thread = new Thread(new ConcurrentBulkInsertThread(indexName, blockNumber, sampleUserData, encData, sleepTime, 1, i));
				threadList.add(thread);
			}
			for (var t: threadList) {
				t.start();
			}
			for (var t: threadList) {
				t.join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	private boolean isIndexExists(String indexName) throws IOException {
		GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
		return esRestClient.getClient().indices().exists(getIndexRequest, RequestOptions.DEFAULT);
	}

	private boolean isBlockExists(String indexName, int blockNumber) throws IOException {
		CountRequest getCorrupedCount = new CountRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.termQuery("block_number", blockNumber));
		getCorrupedCount.source(builder);

		CountResponse response = esRestClient.getClient().count(getCorrupedCount, RequestOptions.DEFAULT);
		return response.getCount() != 0;
	}

	private boolean isEntryExists(String indexName, int blockNumber, int entryNumber) throws IOException {
		CountRequest getCorrupedCount = new CountRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.termQuery("block_number", blockNumber));
		builder.query(QueryBuilders.termQuery("entry_number", entryNumber));
		getCorrupedCount.source(builder);

		CountResponse response = esRestClient.getClient().count(getCorrupedCount, RequestOptions.DEFAULT);
		return response.getCount() != 0;
	}

	private int getMaximumBlockNumber(String indexName) throws IOException {


		SearchRequest searchMaxRequest = new SearchRequest(indexName);
		SearchSourceBuilder maxBuilder = new SearchSourceBuilder();
		maxBuilder.query(QueryBuilders.matchAllQuery());
		MaxAggregationBuilder aggregation =
				AggregationBuilders.max("maxValueAgg").field("block_number");
		maxBuilder.aggregation(aggregation);
		searchMaxRequest.source(maxBuilder);
		SearchResponse response = esRestClient.getClient().search(searchMaxRequest, RequestOptions.DEFAULT);

		if (response.getHits().getTotalHits().value == 0) {
			return -1;
		}


		ParsedMax maxValue = response.getAggregations().get("maxValueAgg");    //get max_aggregation from response
		return (int) maxValue.getValue();
	}

	private int getBlockEntrySize(String indexName, int blockNumber) throws IOException {
		CountRequest request = new CountRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("block_number", blockNumber)));
		request.source(builder);
		CountResponse response = esRestClient.getClient().count(request, RequestOptions.DEFAULT);
		return (int) response.getCount();
	}

	private boolean isDataEquals(List<byte[]> arr1, List<byte[]> arr2) {
		if (arr1.size() != arr2.size())
			return false;
		Boolean[] checkList = new Boolean[arr1.size()];

		for (int i = 0; i < arr1.size(); i++) {
			checkList[i] = Arrays.equals(arr1.get(i), arr2.get(i));
		}
		return Arrays.stream(checkList).allMatch(Boolean::booleanValue);
	}
}
