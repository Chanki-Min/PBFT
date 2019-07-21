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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

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

		List<byte[]> testData = new ArrayList<>();
		try {
			for (int i = 0; i < maxEntryNumber; i++) {
				String str = "test" + (i+threadID);
				testData.add(str.getBytes());
			}

			InputStream in = getClass().getResourceAsStream("/replica.properties");
			Properties prop = new Properties();
			prop.load(in);

			Client client = new Client(prop);

			Operation insertionOp = new InsertionOperation(client.getPublicKey(), testData);
			RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), insertionOp);
			client.request(insertRequestMsg);

			int insertedBlockNum = (int) client.getReply();

			Operation getBlockOp = new GetBlockOperation(client.getPublicKey(), insertedBlockNum);
			RequestMessage getBlockRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), getBlockOp);
			client.request(getBlockRequestMsg);
			List<byte[]> restoredData = (List<byte[]>) client.getReply();

			System.err.print("Thread #" + threadID + " : Original Data : [");
			testData.stream().forEach(x -> System.err.print(new String(x)+", "));
			System.err.print(" ] \n");
			System.err.print("Thread #" + threadID + " : Restored Data : [");
			restoredData.stream().forEach(x -> System.err.print(new String(x)+", "));
			System.err.print(" ] \n");

			Assertions.assertTrue(isDataEquals(testData, restoredData));

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
}

