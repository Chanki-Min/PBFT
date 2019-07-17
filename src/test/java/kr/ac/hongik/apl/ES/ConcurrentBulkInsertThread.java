package kr.ac.hongik.apl.ES;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Assertions;

import java.io.IOError;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class ConcurrentBulkInsertThread extends Thread{
	private final String indexName;
	private final int block_number;
	private final List<byte[]> encrypt_data;
	private final int threadID;
	private final int sleepTime;
	private final int versionNumber;
	private List<byte[]> restoredData = null;
	private EsRestClient esRestClient;


	public ConcurrentBulkInsertThread(String indexName, int block_number, List<byte[]> encrypt_data,int sleepTime, int versionNumber, int threadID){
		this.indexName = indexName;
		this.block_number = block_number;
		this.encrypt_data = encrypt_data;
		this.sleepTime = sleepTime;
		this.versionNumber = versionNumber;
		this.threadID = threadID;
	}

	@Override
	public void run() {
		System.err.println("Thread stated, ThreadNum #"+threadID);
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();

			if (!isIndexExists(indexName)) {
				try {
					esRestClient.createIndex("test_block_chain");
				}catch (EsRestClient.EsConcurrencyException e){
					System.err.println("Thread #"+threadID+" "+e.getClass().toString()+" "+e.getMessage());
				}
			}
			esRestClient.bulkInsertDocument(indexName, block_number, encrypt_data, versionNumber);
			sleep(sleepTime);
			restoredData = esRestClient.getBlockByteArray(indexName, block_number);

			Assertions.assertTrue(isDataEquals(encrypt_data, restoredData));
			esRestClient.deleteIndex(indexName);
			esRestClient.disConnectToEs();

		} catch (IOException e){
			throw new IOError(e);
		} catch (InterruptedException | NoSuchFieldException | EsRestClient.EsException | EsRestClient.EsConcurrencyException e) {
			System.err.println("Thread #"+threadID+" "+e.getMessage());
		}
	}


	private boolean isIndexExists(String indexName) throws IOException{
		GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
		return esRestClient.getClient().indices().exists(getIndexRequest, RequestOptions.DEFAULT);
	}

	private boolean isDataEquals(List<byte[]> arr1, List<byte[]> arr2){
		return arr1.stream().allMatch(by1 -> arr2.stream().anyMatch(by2 -> Arrays.equals(by1, by2))) &&
				arr2.stream().allMatch(by2 -> arr1.stream().anyMatch(by1 -> Arrays.equals(by2, by1))) &&
				arr1.size() == arr2.size();
	}
	private boolean isBlockExists(String indexName, int blockNumber) throws IOException{
		CountRequest getCorrupedCount = new CountRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.termQuery("block_number", blockNumber));
		getCorrupedCount.source(builder);

		CountResponse response = esRestClient.getClient().count(getCorrupedCount, RequestOptions.DEFAULT);
		return response.getCount() != 0;
	}

}
