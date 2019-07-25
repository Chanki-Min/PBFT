package kr.ac.hongik.apl.ES;

import kr.ac.hongik.apl.Client;
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

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.IntStream;

public class AsynchronousInsertionThread extends Thread{
	String indexName = "block_chain";
	int maxEntryNumber = 128;
	int threadID;

	public AsynchronousInsertionThread(String indexName, int maxEntryNumber, int threadID){
		this.indexName = indexName;
		this.maxEntryNumber = maxEntryNumber;
		this.threadID = threadID;
	}

	public void run(){
		System.err.println("Thread #" + threadID + " Started");

		EsJsonParser parser = new EsJsonParser();
		parser.setFilePath("/ES_MappingAndSetting/sample_one_userInfo.json");
		List<Map<String, Object>> sampleUserData = new ArrayList<>();
		try {
			for(int i=0; i<maxEntryNumber; i++) {
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

			int insertedBlockNum = (int) client.getReply();

			Operation getBlockOp = new GetBlockOperation(client.getPublicKey(), insertedBlockNum);
			RequestMessage getBlockRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), getBlockOp);
			client.request(getBlockRequestMsg);
			List<Map<String, Object>> restoredData = (List<Map<String, Object>>) client.getReply();

			System.err.print("Original Data : [");
			sampleUserData.stream().forEach(x -> System.err.print(x + ", "));
			System.err.print(" ] \n");
			System.err.print("Restored Data : [");
			restoredData.stream().forEach(x -> System.err.print(x + ", "));
			System.err.print(" ] \n");

			Assertions.assertTrue(isListMapSame(sampleUserData, restoredData));
		} catch (IOException e) {
			throw new Error(e);
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

	private boolean isDataEquals(List<byte[]> arr1, List<byte[]> arr2){
		if(arr1.size() != arr2.size())
			return false;
		Boolean[] checkList = new Boolean[arr1.size()];

		for(int i=0; i<arr1.size(); i++){
			checkList[i] = Arrays.equals(arr1.get(i), arr2.get(i));
		}
		return Arrays.stream(checkList).allMatch(Boolean::booleanValue);
	}

	private boolean isListMapSame(List<Map<String, Object>> ori, List<Map<String, Object>> res){
		if(ori.size() != res.size())
			return false;

		return IntStream.range(0, ori.size())
				.allMatch(i -> ori.get(i).entrySet().stream()
						.allMatch(e -> e.getValue().equals(res.get(i).get(e.getKey()))));
	}
}

