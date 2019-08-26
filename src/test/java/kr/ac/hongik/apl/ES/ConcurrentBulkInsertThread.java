package kr.ac.hongik.apl.ES;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ConcurrentBulkInsertThread extends Thread {
	private final String indexName;
	private final int block_number;
	private final List<Map<String, Object>> plain_data;
	private final List<byte[]> encrypt_data;
	private final int threadID;
	private final int sleepTime;
	private final int versionNumber;
	private List<byte[]> restoredData = null;
	private EsRestClient esRestClient;


	public ConcurrentBulkInsertThread(String indexName, int block_number, List<Map<String, Object>> plain_data, List<byte[]> encrypt_data, int sleepTime, int versionNumber, int threadID) {
		this.indexName = indexName;
		this.block_number = block_number;
		this.plain_data = plain_data;
		this.encrypt_data = encrypt_data;
		this.sleepTime = sleepTime;
		this.versionNumber = versionNumber;
		this.threadID = threadID;
	}

	@Override
	public void run() {
		System.err.println("Thread stated, ThreadNum #" + threadID);
		EsJsonParser parser = new EsJsonParser();
		esRestClient = new EsRestClient();
		try {
			esRestClient.connectToEs();

			if (!isIndexExists(indexName)) {
				try {
					XContentBuilder mappingBuilder;
					XContentBuilder settingBuilder;
					parser.setFilePath("/ES_MappingAndSetting/ES_mapping_with_plain.json");
					mappingBuilder = parser.jsonFileToXContentBuilder(false);

					parser.setFilePath("/ES_MappingAndSetting/ES_setting_with_plain.json");
					settingBuilder = parser.jsonFileToXContentBuilder(false);

					EsRestClient esRestClient = new EsRestClient();
					esRestClient.connectToEs();
					esRestClient.createIndex(indexName, mappingBuilder, settingBuilder);
				} catch (EsRestClient.EsConcurrencyException e) {
					System.err.println("Thread #" + threadID + " " + e.getClass().toString() + " " + e.getMessage());
				}
			}
			esRestClient.bulkInsertDocumentByProcessor(
					indexName, 0, plain_data, encrypt_data, versionNumber, 100, 10, ByteSizeUnit.MB, 5);
			esRestClient.deleteIndex(indexName);
			esRestClient.disConnectToEs();

		} catch (IOException | InterruptedException | EsRestClient.EsSSLException e) {
			throw new Error(e);
		} catch (NoSuchFieldException | EsRestClient.EsException | EsRestClient.EsConcurrencyException e) {
			System.err.println("Thread #" + threadID + " " + e.getMessage());
		}
	}

	private boolean isIndexExists(String indexName) throws IOException {
		GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
		return esRestClient.getClient().indices().exists(getIndexRequest, RequestOptions.DEFAULT);
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

	private boolean isBlockExists(String indexName, int blockNumber) throws IOException {
		CountRequest getCorrupedCount = new CountRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.termQuery("block_number", blockNumber));
		getCorrupedCount.source(builder);

		CountResponse response = esRestClient.getClient().count(getCorrupedCount, RequestOptions.DEFAULT);
		return response.getCount() != 0;
	}

}
