package kr.ac.hongik.apl.ES;


import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;

public class ElasticSearchTest {

	@Test
	void esConnectionTest(){
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		}
		boolean isConnected = false;

		try {
			isConnected = !esRestClient.getClusterInfo().isTimedOut();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("ClusterInfo Requset successful? : "+ isConnected);
		try {
			esRestClient.disConnectToEs();
		} catch (IOException e) {
			e.printStackTrace();
		}

		Assertions.assertEquals(true, isConnected);
	}

	@Test
	void indexCreate_DeleteTest(){
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		}

		try {
			XContentBuilder mappingBuilder = new XContentFactory().jsonBuilder();
			mappingBuilder.startObject();
			{
				mappingBuilder.startObject("properties");
				{
					mappingBuilder.startObject("test_block_number");
					{
						mappingBuilder.field("type", "long");
						mappingBuilder.field("coerce", false);            //forbid auto-casting String to Integer
						mappingBuilder.field("ignore_malformed", true);    //forbid non-numeric values
					}
					mappingBuilder.endObject();

					mappingBuilder.startObject("test_entry_number");
					{
						mappingBuilder.field("type", "long");
						mappingBuilder.field("coerce", false);
						mappingBuilder.field("ignore_malformed", true);
					}
					mappingBuilder.endObject();

					mappingBuilder.startObject("test_encrypt_data");
					{
						mappingBuilder.field("type", "binary");
					}
					mappingBuilder.endObject();
				}
				mappingBuilder.endObject();
				mappingBuilder.field("dynamic", "strict");    //forbid auto field creation
			}
			mappingBuilder.endObject();
			XContentBuilder settingsBuilder = new XContentFactory().jsonBuilder();
			settingsBuilder.startObject();
			{
				settingsBuilder.field("index.number_of_shards", 4);
				settingsBuilder.field("index.number_of_replicas", 3);
				settingsBuilder.field("index.merge.scheduler.max_thread_count", 1);
			}
			settingsBuilder.endObject();

			esRestClient.createIndex("test_block_chain",mappingBuilder,settingsBuilder);

			Assertions.assertEquals(true, isIndexExists("test_block_chain"));
			System.out.println("Index Creation test Successful, deleting test-index...");
			esRestClient.deleteIndex("test_block_chain");
			Assertions.assertEquals(false, isIndexExists("test_block_chain"));
			esRestClient.disConnectToEs();

		}catch (Exception e){
			e.printStackTrace();
		}
	}

	@Test
	void BulkInsertTest(){
		int blockNumber = 0;
		int entrySize = 5;
		int sleepTime = 1000;
		int versionNuber = 1;
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();
			esRestClient.createIndex("test_block_chain");

			List<byte[]> testBinaryList = new ArrayList<>();
			for (int i = 0; i < entrySize; i++) {
				String data = "test" + i;
				testBinaryList.add(data.getBytes());
			}
			esRestClient.bulkInsertDocument("test_block_chain", blockNumber, testBinaryList, versionNuber);
			sleep(sleepTime);

			Assertions.assertTrue(isBlockExists("test_block_chain",blockNumber));
			Assertions.assertEquals(entrySize, getBlockEntrySize("test_block_chain", blockNumber) -1);
			esRestClient.deleteIndex("test_block_chain");
		} catch (InterruptedException | EsRestClient.EsConcurrencyException | EsRestClient.EsException | NoSuchFieldException e) {
			e.printStackTrace();
		} catch (IOException e){
			throw new IOError(e);
		}
	}

	@Test
	void getBlockByteArrayTest(){
		int blockNumber = 0;
		int entrySize = 10000;
		int sleepTime = 1000;
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();
			esRestClient.createIndex("test_block_chain");

			List<byte[]> testBinaryList = new ArrayList<>();
			for (int i = 0; i < entrySize; i++) {
				String data = "test" + i;
				testBinaryList.add(data.getBytes());
			}
			esRestClient.bulkInsertDocument("test_block_chain", blockNumber, testBinaryList, 1);

			sleep(sleepTime);

			List<byte[]> restoredByteList = esRestClient.getBlockByteArray("test_block_chain",0);

			Assertions.assertTrue(isDataEquals(testBinaryList, restoredByteList));


			esRestClient.deleteIndex("test_block_chain");
		} catch (InterruptedException | EsRestClient.EsConcurrencyException | EsRestClient.EsException | NoSuchFieldException e) {
			e.printStackTrace();
		} catch (IOException e) {
			throw new IOError(e);
		}
	}

	@Test
	void getBlockEntryByteArrayTest(){
		int blockNumber = 0;
		int entrySize = 5000;
		int sleepTime = 1000;
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();
			esRestClient.createIndex("test_block_chain");

			List<byte[]> testBinaryList = new ArrayList<>();
			for (int i = 0; i < entrySize; i++) {
				String data = "test" + i;
				testBinaryList.add(data.getBytes());
			}
			esRestClient.bulkInsertDocument("test_block_chain", blockNumber, testBinaryList, 1);

			sleep(sleepTime);

			List<byte[]> restoredByteList = new ArrayList<>();

			IntStream.range(0,testBinaryList.size()).forEach(i -> {
				try {
					restoredByteList.add(esRestClient.getBlockEntryByteArray("test_block_chain", blockNumber, i));
				} catch (IOException e) {
					e.printStackTrace();
				} catch (EsRestClient.EsException e) {
					e.printStackTrace();
				}
			});
			List<String> testStringList = new ArrayList<>();
			List<String> restoredStringList = new ArrayList<>();
			for(int i = 0; i<testBinaryList.size(); i++){
				testStringList.add(new String(testBinaryList.get(i)));
				restoredStringList.add(new String(restoredByteList.get(i)));
			}
			Assertions.assertEquals(true, restoredStringList.containsAll(testStringList)
					&& restoredStringList.size() == testStringList.size());


			esRestClient.deleteIndex("test_block_chain");
		} catch (InterruptedException | EsRestClient.EsConcurrencyException | EsRestClient.EsException | NoSuchFieldException e) {
			e.printStackTrace();
		} catch (IOException e) {
			throw new IOError(e);
		}
	}

	@Test
	void largeBinaryFileBulkInsertTest(){
		int blockNumber = 0;
		int entrySize = 5000;
		int sleepTime = 1000;
		String filePath = "C:\\Users\\Chanki_Min\\Desktop\\ESmodule\\src\\main\\resources\\sample_binary_4kb";
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();
			esRestClient.createIndex("test_block_chain");

			List<byte[]> testBinaryList = new ArrayList<>();
			File file = new File(filePath);
			FileInputStream fis = null;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			fis = new FileInputStream(file);

			int len = 0;
			byte[] buf = new byte[1024];

			while ((len = fis.read(buf)) != -1) {
				baos.write(buf, 0, len);
			}

			byte[] fileArray = baos.toByteArray();
			for (int i = 0; i < entrySize; i++) {
				testBinaryList.add(fileArray);
			}
			esRestClient.bulkInsertDocument("test_block_chain", blockNumber, testBinaryList, 1);

			sleep(sleepTime);

			List<byte[]> restoredByteList = new ArrayList<>();
			restoredByteList = esRestClient.getBlockByteArray("test_block_chain",0);

			List<String> testStringList = new ArrayList<>();
			List<String> restoredStringList = new ArrayList<>();
			for(int i = 0; i<testBinaryList.size(); i++){
				testStringList.add(new String(testBinaryList.get(i)));
				restoredStringList.add(new String(restoredByteList.get(i)));
			}
			Assertions.assertEquals(true, restoredStringList.containsAll(testStringList)
					&& restoredStringList.size() == testStringList.size());


			esRestClient.deleteIndex("test_block_chain");
		} catch (InterruptedException | EsRestClient.EsConcurrencyException | EsRestClient.EsException | NoSuchFieldException e) {
			e.printStackTrace();
		} catch (IOException e) {
			throw new IOError(e);
		}
	}

	@Test
	void concurrentBulkInsertTest(){
		int entrySize = 1000;
		int sleepTime = 1000;
		int maxThreadNum = 10;
		List<byte[]> testBinaryList = new ArrayList<>();
		List<Thread> threadList = new ArrayList<>(maxThreadNum);
		String indexName = "test_block_chain";
		int blockNumber = 0;

		System.err.println("EStest: concurrentBulkInsert");
		for (int i = 0; i < entrySize; i++) {
			//String data = "test" + i;
			String data = "test";
			testBinaryList.add(data.getBytes());
		}

		try {
			for(int i=0; i<maxThreadNum; i++){
				Thread thread = new Thread(new ConcurrentBulkInsertThread(indexName,blockNumber,testBinaryList,sleepTime,1,i));
				threadList.add(thread);
			}
			for(var t : threadList){
				t.start();
			}
			for(var t : threadList){
				t.join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static EsRestClient esRestClient;

	private boolean isIndexExists(String indexName) throws IOException{
		GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
		return esRestClient.getClient().indices().exists(getIndexRequest, RequestOptions.DEFAULT);
	}

	private boolean isBlockExists(String indexName, int blockNumber) throws IOException{
		CountRequest getCorrupedCount = new CountRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.termQuery("block_number", blockNumber));
		getCorrupedCount.source(builder);

		CountResponse response = esRestClient.getClient().count(getCorrupedCount, RequestOptions.DEFAULT);
		return response.getCount() != 0;
	}

	private boolean isEntryExists(String indexName, int blockNumber, int entryNumber) throws IOException{
		CountRequest getCorrupedCount = new CountRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.termQuery("block_number", blockNumber));
		builder.query(QueryBuilders.termQuery("entry_number", entryNumber));
		getCorrupedCount.source(builder);

		CountResponse response = esRestClient.getClient().count(getCorrupedCount, RequestOptions.DEFAULT);
		return response.getCount() != 0;
	}

	private int getMaximumBlockNumber(String indexName) throws IOException{



		SearchRequest searchMaxRequest = new SearchRequest(indexName);
		SearchSourceBuilder maxBuilder = new SearchSourceBuilder();
		maxBuilder.query(QueryBuilders.matchAllQuery());
		MaxAggregationBuilder aggregation =
				AggregationBuilders.max("maxValueAgg").field("block_number");
		maxBuilder.aggregation(aggregation);
		searchMaxRequest.source(maxBuilder);
		SearchResponse response = esRestClient.getClient().search(searchMaxRequest, RequestOptions.DEFAULT);

		if(response.getHits().getTotalHits().value == 0){
			return -1;
		}


		ParsedMax maxValue = response.getAggregations().get("maxValueAgg");	//get max_aggregation from response
		return (int) maxValue.getValue();
	}

	private int getBlockEntrySize(String indexName, int blockNumber) throws IOException{
		CountRequest request = new CountRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("block_number", blockNumber)));
		request.source(builder);
		CountResponse response = esRestClient.getClient().count(request, RequestOptions.DEFAULT);
		return (int) response.getCount();
	}

	private boolean isDataEquals(List<byte[]> arr1, List<byte[]> arr2){
		if(arr1.size() != arr2.size())
			return false;
		Boolean[] checkList = new Boolean[arr1.size()];

		for(int i=0; i<arr1.size(); i++){
			checkList[i] = Arrays.equals(arr1.get(i), arr2.get(i));
		}
		return Arrays.stream(checkList).allMatch(Boolean::booleanValue);
	}
}
