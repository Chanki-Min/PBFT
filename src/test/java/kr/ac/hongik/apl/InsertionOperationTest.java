package kr.ac.hongik.apl;

import kr.ac.hongik.apl.ES.AsynchronousInsertionThread;
import kr.ac.hongik.apl.ES.EsJsonParser;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.GetBlockOperation;
import kr.ac.hongik.apl.Operations.InsertionOperation;
import kr.ac.hongik.apl.Operations.Operation;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;

public class InsertionOperationTest {
	@Test
	public void SynchronizedInsertionTest(){
		final int loop = 25;
		Boolean[] checkList = new Boolean[loop];

		for(int i=0; i<loop; i++){
			checkList[i] = oneClientInsertionTester();
		}
		Assertions.assertTrue(Arrays.stream(checkList).allMatch(Boolean::booleanValue));
	}

	@Test
	public void ManyManyAsyncInsertionTest(){
		final int loop = 10;

		long[] eachTookTime = new long[loop];
		for(int i=0; i<loop; i++){
			long originTime = System.currentTimeMillis();
			AsynchronousInsertionTest();
			eachTookTime[i] = (System.currentTimeMillis() - originTime);
		}
		double average = Arrays.stream(eachTookTime).average().getAsDouble();

		System.err.println("All AsyncInsertionTest FINISH");
		System.err.print("EachTookTime : [");
		Arrays.stream(eachTookTime).forEach(t -> System.err.print(t + "ms, ")); System.err.println("]");

		System.err.println("AverageTookTime of Test :" + average);
		System.err.println("AverageTookTime of One Insertion & GetDecrypt :" + (average/10.0) );
	}

	@Test
	public void AsynchronousInsertionTest(){
		final int maxEntryNum = 10;
		final int maxThreadNum = 10;
		List<Thread> threadList = new ArrayList<>(maxThreadNum);
		String indexName = "block_chain";

		System.err.println("InsertionOpTest::Asynchronous");
		try {
			for(int i=0; i<maxThreadNum; i++){
				Thread thread = new Thread(new AsynchronousInsertionThread(indexName,maxEntryNum,i));
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

	public boolean oneClientInsertionTester(){
		final String indexName = "block_chain";
		final int entryNumber = 128;
		final long sleepTime = 0;
		int blockNumberToGet;
		EsJsonParser parser = new EsJsonParser();
		parser.setFilePath("/ES_MappingAndSetting/sample_one_userInfo.json");
		List<Map<String, Object>> sampleUserData = new ArrayList<>();
		try {
			blockNumberToGet = getLatestBlockNumber(indexName) +1;
			System.err.println("blockNumber2Get :"+blockNumberToGet);
			for(int i=0; i<entryNumber; i++) {
				var map = parser.jsonToMap();
				map.put("start_time", String.valueOf(System.currentTimeMillis()));
				sampleUserData.add(map);
			}

			InputStream in = getClass().getResourceAsStream("/replica.properties");
			Properties prop = new Properties();
			prop.load(in);

			Client client = new Client(prop);

			Operation insertionOp = new InsertionOperation(client.getPublicKey(), sampleUserData);
			RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), insertionOp);
			client.request(insertRequestMsg);
			int  result = (int) client.getReply();

			sleep(sleepTime);

			Operation getBlockOp = new GetBlockOperation(client.getPublicKey(), blockNumberToGet);
			RequestMessage getBlockRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), getBlockOp);
			client.request(getBlockRequestMsg);
			List<Map<String, Object>> restoredData = (List<Map<String, Object>>) client.getReply();

			System.err.print("Original Data : [");
			sampleUserData.stream().forEach(x -> System.err.print(x + ", "));
			System.err.print(" ] \n");
			System.err.print("Restored Data : [");
			restoredData.stream().forEach(x -> System.err.print(x + ", "));
			System.err.print(" ] \n");

			Assertions.assertEquals(blockNumberToGet, result);
			Assertions.assertTrue(isListMapSame(sampleUserData, restoredData));
			if(isListMapSame(sampleUserData, restoredData))
				return true;

		} catch (IOException | InterruptedException | NoSuchFieldException e) {
			throw new Error(e);
		}
		return false;
	}

	public void clearEsDB(String indexName) throws IOException, EsRestClient.EsException, NoSuchFieldException{
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();
		if(esRestClient.isIndexExists(indexName)){
			esRestClient.deleteIndex(indexName);
			System.err.println("clearEsDB::index :"+indexName+" was already exists, DELETE index");
		}
	}

	private int getLatestBlockNumber(String indexName) throws NoSuchFieldException, IOException{
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		if(esRestClient.isIndexExists(indexName)) {
			SearchRequest searchMaxRequest = new SearchRequest(indexName);
			SearchSourceBuilder maxBuilder = new SearchSourceBuilder();

			maxBuilder.query(QueryBuilders.matchAllQuery());
			MaxAggregationBuilder aggregation = AggregationBuilders.max("maxValueAgg").field("block_number");
			maxBuilder.aggregation(aggregation);

			searchMaxRequest.source(maxBuilder);
			SearchResponse response = esRestClient.getClient().search(searchMaxRequest, RequestOptions.DEFAULT);

			if (response.getHits().getTotalHits().value == 0) {
				return 0;
			}
			ParsedMax maxValue = response.getAggregations().get("maxValueAgg");    //get max_aggregation from response
			return (int) maxValue.getValue();
		}
		else
			return 0;
	}

	private boolean isListMapSame(List<Map<String, Object>> ori, List<Map<String, Object>> res){
		if(ori.size() != res.size())
			return false;

		return IntStream.range(0, ori.size())
				.allMatch(i -> ori.get(i).entrySet().stream()
						.allMatch(e -> e.getValue().equals(res.get(i).get(e.getKey()))));
	}
}
