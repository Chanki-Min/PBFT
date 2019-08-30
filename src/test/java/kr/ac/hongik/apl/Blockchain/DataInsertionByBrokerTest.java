package kr.ac.hongik.apl.Blockchain;

import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.ES.EsJsonParser;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.InsertHeaderOperation;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Util;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static kr.ac.hongik.apl.Util.makeSymmetricKey;

public class DataInsertionByBrokerTest {
	@Test
	public void nonBlockChainDataInsertionTest() throws NoSuchFieldException, EsRestClient.EsSSLException, IOException, EsRestClient.EsConcurrencyException, EsRestClient.EsException, InterruptedException {
		boolean deleteIndicesAfterFinish = false;
		/*
		이 테스트에서는 Block Chain에 넣지 않을 일반적인 data의 insertion을 테스트한다.
		이전까지 Broker가 MQ에 쌓인 데이터를 처리하여 resource/Es+userData/~.json에 data를 정리하였다고
		가정한다.
		 */
		Map<String, String> mappingToData = new HashMap<>();
		mappingToData.put("/ES_MappingAndSetting/Mapping_car_specification.json", "/Es_userData/Data_car_specification.json");
		mappingToData.put("/ES_MappingAndSetting/Mapping_id_to_car.json", "/ES_userData/Data_id_to_car.json");
		mappingToData.put("/ES_MappingAndSetting/Mapping_user_info.json", "/Es_userData/Data_user_info.json");
		String settingPath = "/ES_MappingAndSetting/ES_setting_with_plain.json";

		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		//create indices
		for (var mapping: mappingToData.keySet()) {
			String indexName = getIndexNameFromFilePath(mapping);
			if (!esRestClient.isIndexExists(indexName)) {
				EsJsonParser esJsonParser = new EsJsonParser();
				esJsonParser.setFilePath(mapping);
				XContentBuilder mappingBuilder = esJsonParser.jsonFileToXContentBuilder(false);
				esJsonParser.setFilePath(settingPath);
				XContentBuilder settingBuilder = esJsonParser.jsonFileToXContentBuilder(false);
				esRestClient.createIndex(indexName, mappingBuilder, settingBuilder);
			}
		}

		//insert data
		for (var mapping: mappingToData.keySet()) {
			String indexName = getIndexNameFromFilePath(mapping);
			EsJsonParser esJsonParser = new EsJsonParser();
			esJsonParser.setFilePath(mappingToData.get(mapping));

			List data = esJsonParser.listedJsonFileToList("data_" + indexName);
			bulkInsertNonBlockChainData(indexName, data, 10000, 10, ByteSizeUnit.MB, 1);
		}

		if (deleteIndicesAfterFinish) {
			for (var mapping: mappingToData.keySet()) {
				String indexName = getIndexNameFromFilePath(mapping);
				esRestClient.deleteIndex(indexName);
			}
		}
		esRestClient.disConnectToEs();
	}

	@Test
	public void blockChainDataInsertionTest() throws NoSuchFieldException, EsRestClient.EsSSLException, IOException, EsRestClient.EsConcurrencyException, EsRestClient.EsException, Util.EncryptionException, InterruptedException {
		boolean deleteIndicesAfterFinish = false;
		int dataDuplication = 10000;
		Map<String, Long> times = new HashMap<>();
		/*
		이 테스트에서는 Block Chain에 포함되는 data의 insertion을 테스트한다.
		이전까지 Broker가 MQ에 쌓인 데이터를 처리하여 resource/Es+userData/~.json에 data를 정리하였다고 가정한다.
		 */
		Map<String, String> mappingToData = new HashMap<>();
		mappingToData.put("/ES_MappingAndSetting/Mapping_car_log.json", "/Es_userData/Data_car_log.json");
		mappingToData.put("/ES_MappingAndSetting/Mapping_user_log.json", "/ES_userData/Data_user_log.json");
		String settingPath = "/ES_MappingAndSetting/ES_setting_with_plain.json";

		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		//create indices
		for (var mapping: mappingToData.keySet()) {
			String indexName = getIndexNameFromFilePath(mapping);
			times.put("index_creation_start", System.currentTimeMillis());

			if (!esRestClient.isIndexExists(indexName)) {
				EsJsonParser esJsonParser = new EsJsonParser();
				esJsonParser.setFilePath(mapping);
				XContentBuilder mappingBuilder = esJsonParser.jsonFileToXContentBuilder(false);
				esJsonParser.setFilePath(settingPath);
				XContentBuilder settingBuilder = esJsonParser.jsonFileToXContentBuilder(false);
				esRestClient.createIndex(indexName, mappingBuilder, settingBuilder);

				times.put("index_creation_end", System.currentTimeMillis());
				System.err.println(indexName + " index generation ends with " +
						(times.get("index_creation_start") - times.get("index_creation_end")) +
						"ms");
			}
		}

		List car_logs = new ArrayList(), user_logs = new ArrayList();
		for (var mapping: mappingToData.keySet()) {
			String indexName = getIndexNameFromFilePath(mapping);
			EsJsonParser esJsonParser = new EsJsonParser();
			esJsonParser.setFilePath(mappingToData.get(mapping));
			if (indexName.equals("car_log"))
				car_logs.addAll(esJsonParser.listedJsonFileToList("data_" + indexName));
			else if (indexName.equals("user_log"))
				user_logs.addAll(esJsonParser.listedJsonFileToList("data_" + indexName));
		}
		Assertions.assertNotNull(car_logs, "car_log data load fail");
		Assertions.assertNotNull(user_logs, "user_log data load fail");


		//while: 2개의 json 파일에서 임의의 순서로 다음단계에 넣을 데이터를 고르기
		int i = 0, j = 0;
		while (true) {
			//load data from json by random
			long rand = Math.round(Math.random());
			List<Map<String, Object>> dataList = new ArrayList<>();
			String indexName;
			if (i == car_logs.size() && j == user_logs.size()) {
				break;
			}
			if (rand == 1) {
				if (i != car_logs.size()) {
					for (int k = 0; k < dataDuplication; k++) {
						dataList.addAll((List<Map<String, Object>>) car_logs.get(i));
					}
					indexName = "car_log";
					i++;
				} else {
					for (int k = 0; k < dataDuplication; k++) {
						dataList.addAll((List<Map<String, Object>>) user_logs.get(j));
					}
					indexName = "user_log";
					j++;
				}
			} else {
				if (j != user_logs.size()) {
					for (int k = 0; k < dataDuplication; k++) {
						dataList.addAll((List<Map<String, Object>>) user_logs.get(j));
					}
					indexName = "user_log";
					j++;
				} else {
					for (int k = 0; k < dataDuplication; k++) {
						dataList.addAll((List<Map<String, Object>>) car_logs.get(i));
					}
					indexName = "car_log";
					i++;
				}
			}
			//get latest block# from ElasticSearch
			int blockNumber = getLatestBlockNumber(Arrays.asList("car_log", "user_log"));
			blockNumber++;
			times.put("start", System.currentTimeMillis());

			//create all data [block#, entry#, cipher, planMap]
			HashTree hashTree = new HashTree(dataList.stream().map(x -> (Serializable) x).map(Util::hash).toArray(String[]::new));
			String root = hashTree.toString();

			SecretKey key = makeSymmetricKey(root);
			List<byte[]> cipher = new ArrayList<>();
			for (Map<String, Object> x: dataList) {
				byte[] encrypt = Util.encrypt(Util.serToString((Serializable) x).getBytes(), key);
				cipher.add(encrypt);
			}
			times.put("dataCreation", System.currentTimeMillis());

			//insert [block#, entry#, cipher, planMap] to ElasticSearch
			esRestClient.bulkInsertDocumentByProcessor(indexName, blockNumber, dataList, cipher,
					1, 1000, 10, ByteSizeUnit.MB, 5);
			times.put("esInsertion", System.currentTimeMillis());

			//send [block#, root] to PBFT to PBFT generates Header and store to sqliteDB itself
			InputStream in = getClass().getResourceAsStream("/replica.properties");
			Properties prop = new Properties();
			prop.load(in);
			Client client = new Client(prop);

			Operation insertHeaderOp = new InsertHeaderOperation(client.getPublicKey(), blockNumber, root);
			RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), insertHeaderOp);
			client.request(insertRequestMsg);
			int result = (int) client.getReply();
			times.put("pbftRootInsertion", System.currentTimeMillis());
			Assertions.assertEquals(blockNumber, result);

			System.err.println("Block #" + blockNumber + ", index: " + indexName);
			System.err.println("data creation end with: " + (times.get("dataCreation") - times.get("start")));
			System.err.println("es insertion end with: " + (times.get("esInsertion") - times.get("dataCreation")));
			System.err.println("pbft root insertion end with: " + (times.get("pbftRootInsertion") - times.get("esInsertion")));
			System.err.println("es+pbft end with: " + (times.get("pbftRootInsertion") - times.get("dataCreation")));
			System.err.println("===============================");
		}

		if(deleteIndicesAfterFinish)
			for (var mapping: mappingToData.keySet()) {
				String indexName = getIndexNameFromFilePath(mapping);
				esRestClient.deleteIndex(indexName);
			}
		esRestClient.disConnectToEs();
	}

	/**
	 * @param fileName String formatted ~Mapping_[target]*.json
	 * @return [target]*
	 */
	private String getIndexNameFromFilePath(String fileName) {
		Pattern pattern = Pattern.compile("(?<=(Mapping_)).*(?=(.json))");
		Matcher matcher = pattern.matcher(fileName);
		if (matcher.find())
			return matcher.group();
		else
			return null;
	}

	/**
	 * @param data id를 생성할 엔트리 데이터
	 * @param indexName 엔트리가 속한 인덱스 이름
	 * @return /ES_MappingAndSetting/Mapping_primary_key.json에 정의된 primary_key로 구한 "data.get(primary_key)_..."
	 */
	private String getNonBlockChainID(Map data, String indexName) {
		EsJsonParser esJsonParser = new EsJsonParser();
		esJsonParser.setFilePath("/ES_MappingAndSetting/Mapping_primary_key.json");
		List list = (List) esJsonParser.jsonFileToMap().get(indexName);

		StringBuilder builder = new StringBuilder();
		list.stream().map(x -> data.get(x)).forEachOrdered(x -> builder.append(x).append("_"));
		return builder.substring(0, builder.length() - 1);
	}

	/**
	 * @param indices list of search target indices
	 * @return latest block_number of indices
	 * @throws NoSuchFieldException
	 * @throws IOException
	 * @throws EsRestClient.EsSSLException
	 */
	private int getLatestBlockNumber(List<String> indices) throws NoSuchFieldException, IOException, EsRestClient.EsSSLException {
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		SearchRequest searchMaxRequest = new SearchRequest(indices.toArray(new String[indices.size()]));
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

	private void bulkInsertNonBlockChainData(String indexName, List<Map<String, Object>> dataList
			, int maxAction, int maxSize, ByteSizeUnit maxSizeUnit, int threadSize) throws NoSuchFieldException, EsRestClient.EsSSLException, IOException, EsRestClient.EsException, InterruptedException {
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		if (!esRestClient.isIndexExists(indexName))
			throw new EsRestClient.EsException("index :" + indexName + " does not exists");

		Set indexKeySet = esRestClient.getFieldKeySet(indexName);
		if (!dataList.stream().allMatch(x -> x.keySet().equals(indexKeySet)))
			throw new EsRestClient.EsException("index :" + indexName + " field mapping does NOT equal to given dataList ketSet");

		BulkProcessor.Listener listener = new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
				System.err.println("bulk insertion START, LEN :" + request.numberOfActions() + " SIZE :" + request.estimatedSizeInBytes());
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request,
								  BulkResponse response) {
				System.err.println("bulk insertion OK, LEN :" + request.numberOfActions() + " SIZE :" + request.estimatedSizeInBytes() + " exeID :" + executionId);
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request,
								  Throwable failure) {
				System.err.println("bulk insertion FAIL, cause :" + failure);
			}
		};
		BulkProcessor.Builder processorBuilder = BulkProcessor.builder(
				(req, bulkListener) ->
						esRestClient.getClient().bulkAsync(req, RequestOptions.DEFAULT, bulkListener), listener);
		processorBuilder.setBulkActions(maxAction);
		processorBuilder.setBulkSize(new ByteSizeValue(maxSize, maxSizeUnit));
		processorBuilder.setConcurrentRequests(threadSize);
		processorBuilder.setBackoffPolicy(BackoffPolicy
				.constantBackoff(TimeValue.timeValueSeconds(1L), 3));
		BulkProcessor bulkProcessor = processorBuilder.build();


		for (int entryNumber = 0; entryNumber < dataList.size(); entryNumber++) {
			XContentBuilder builder = XContentFactory.jsonBuilder();
			builder.startObject();
			for (String key: dataList.get(entryNumber).keySet()) {
				builder.field(key, dataList.get(entryNumber).get(key));
			}
			builder.endObject();
			bulkProcessor.add(new IndexRequest(indexName).id(getNonBlockChainID(dataList.get(entryNumber), indexName)).source(builder));
		}
		bulkProcessor.awaitClose(10L, TimeUnit.SECONDS);
	}
}
