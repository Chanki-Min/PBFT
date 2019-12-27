package kr.ac.hongik.apl.ES;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ConcurrentBulkInsertThread extends Thread {
	private EsRestClient esRestClient;
	private Map<String, Object> esRestClientConfigs;
	private final String indexName;
	private final List<Map<String, Object>> plain_data;
	private final List<byte[]> encrypt_data;
	private final int threadID;
	private final int versionNumber;
	private final String mappingPath = "/ES_MappingAndSetting/Debug_test_mapping.json";
	private final String settingPath = "/ES_MappingAndSetting/Setting.json";

	public ConcurrentBulkInsertThread(Map esRestClientConfigs, String indexName, int block_number, List<Map<String, Object>> plain_data, List<byte[]> encrypt_data, int sleepTime, int versionNumber, int threadID) {
		this.esRestClientConfigs = esRestClientConfigs;
		this.indexName = indexName;
		this.plain_data = plain_data;
		this.encrypt_data = encrypt_data;
		this.versionNumber = versionNumber;
		this.threadID = threadID;
	}

	@Override
	public void run() {
		System.err.println("Thread stated, ThreadNum #" + threadID);
		EsJsonParser parser = new EsJsonParser();
		try {
			esRestClient = new EsRestClient(esRestClientConfigs);
			esRestClient.connectToEs();

			if (!isIndexExists(indexName)) {
				try {
					XContentBuilder mappingBuilder;
					XContentBuilder settingBuilder;
					mappingBuilder = parser.jsonFileToXContentBuilder(mappingPath,false);

					settingBuilder = parser.jsonFileToXContentBuilder(settingPath,false);

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
}
