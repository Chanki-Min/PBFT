package kr.ac.hongik.apl.Operation;

import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.Blockchain.HashTree;
import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.ES.EsJsonParser;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Generator.Generator;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.BlockVerificationOperation;
import kr.ac.hongik.apl.Operations.GetHeaderOperation;
import kr.ac.hongik.apl.Operations.InsertHeaderOperation;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Util;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static kr.ac.hongik.apl.Util.makeSymmetricKey;

public class BlockVerificationOperationTest {
	private Map<String, Object> esRestClientConfigs;
	private EsRestClient esRestClient;
	private ObjectMapper objectMapper = new ObjectMapper();

	@BeforeEach
	public void makeConfig() throws NoSuchFieldException, EsRestClient.EsSSLException {
		esRestClientConfigs = new HashMap<>();
		esRestClientConfigs.put("userName", "apl");
		esRestClientConfigs.put("passWord", "wowsan2015@!@#$");
		esRestClientConfigs.put("certPath", "/ES_Connection/esRestClient-cert.p12");
		esRestClientConfigs.put("certPassWord", "wowsan2015@!@#$");

		Map<String, Object> masterMap = new HashMap<>();
		masterMap.put( "name", "es01-master01");
		masterMap.put( "hostName", "223.194.70.105");
		masterMap.put( "port", "19192");
		masterMap.put( "hostScheme", "https");

		esRestClientConfigs.put("masterHostInfo", List.of(masterMap));
		esRestClient = new EsRestClient(esRestClientConfigs);
		esRestClient.connectToEs();
	}

	@AfterEach
	public void disconnect() throws IOException {
		esRestClient.disConnectToEs();
	}

	@Test
	public void verifyBlockOpTest() throws IOException {
		List<String> indices = Arrays.asList("car_log", "user_log");
		int blockNumber = 1;

		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);

		Operation verifyBlockOp = new BlockVerificationOperation(client.getPublicKey(), esRestClientConfigs, blockNumber, indices);
		RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), verifyBlockOp);
		long send = System.currentTimeMillis();
		client.request(insertRequestMsg);
		List result = (List) client.getReply();

		System.err.printf("got reply from PBFT with time %dms\n", (System.currentTimeMillis() - send));
		int y = 0;
	}

	@Test
	public void makeBlockAndVerifyBlockTest() throws Exception {
		String dummyDataPath = "/GEN_initData/Init_data00.json";
		String indexMappingPath = "/ES_MappingAndSetting/Mapping_car_log.json";
		String indexSettingPath = "/ES_MappingAndSetting/Setting.json";
		String indexName = getIndexNameFromFilePath(indexMappingPath);
		boolean isDataLoop = true;
		int dataSize = 100;

		Generator generator = new Generator(dummyDataPath, isDataLoop);
		List<Map<String, Object>> carLogList = new ArrayList<>();
		for(int i=0; i<dataSize; i++) {
			carLogList.add(generator.generate().getLeft());
		}

		if(!esRestClient.isIndexExists(indexName)) {
			EsJsonParser esJsonParser = new EsJsonParser();
			XContentBuilder mappingBuilder = esJsonParser.jsonFileToXContentBuilder(indexMappingPath, false);
			XContentBuilder settingBuilder = esJsonParser.jsonFileToXContentBuilder(indexSettingPath, false);
			esRestClient.createIndex(indexName, mappingBuilder, settingBuilder);
		}

		int blockNumber = getLatestBlockNumber(Arrays.asList("car_log")) + 1;

		HashTree hashTree = storeDataToEs(indexName, blockNumber, carLogList);
		storeHeaderToPBFT(blockNumber, hashTree.toString());
		System.err.printf("car_log insertion end\n");

		/*
		데이터 삽입 종료, 무결한 상태의 블록을 검증해본다
		 */

		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);

		Operation verifyBlockOp = new BlockVerificationOperation(client.getPublicKey(), esRestClientConfigs, blockNumber, Arrays.asList("car_log"));
		RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), verifyBlockOp);
		long send = System.currentTimeMillis();
		client.request(insertRequestMsg);
		List noErrorResult = (List) client.getReply();

		/*
		데이터를 변조한 뒤에 다시 검증해본다
		 */

		verifyBlockOp = new BlockVerificationOperation(client.getPublicKey(), esRestClientConfigs, blockNumber, Arrays.asList("car_log"));
		insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), verifyBlockOp);
		client.request(insertRequestMsg);
		List errorResult = (List) client.getReply();

	}

	private String getIndexNameFromFilePath(String fileName) {
		Pattern pattern = Pattern.compile("(?<=(Mapping_)).*(?=(.json))");
		Matcher matcher = pattern.matcher(fileName);
		if (matcher.find())
			return matcher.group();
		else
			return null;
	}

	private int getLatestBlockNumber(List<String> indices) throws NoSuchFieldException, IOException, EsRestClient.EsSSLException {
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

	private HashTree storeDataToEs(String indexName, int blockNumber, List<Map<String, Object>> dataList) throws IOException, Util.EncryptionException, EsRestClient.EsConcurrencyException, InterruptedException, EsRestClient.EsException {
		//get previous Header from PBFT
		Triple<Integer, String, String> prevHeader = getHeader(blockNumber - 1);

		//create all data [block#, entry#, cipher, planMap]
		List<String> list = new ArrayList<>();
		for (Map<String, Object> stringObjectMap: dataList) {
			String s = objectMapper.writeValueAsString(stringObjectMap);
			String hash = Util.hash(s);
			list.add(hash);
		}
		HashTree hashTree = new HashTree(list.toArray(new String[0]));
		Triple<Integer, String, String> currHeader = new ImmutableTriple<>(blockNumber, hashTree.toString(), Util.hash(prevHeader.toString()));
		SecretKey key = makeSymmetricKey(currHeader.toString());

		System.err.println("currHeader = " + currHeader.toString());

		List<byte[]> cipher = new ArrayList<>();
		for (Map<String, Object> x: dataList) {
			byte[] encrypt = Util.encrypt(Util.serToBase64String((Serializable) x).getBytes(), key);
			cipher.add(encrypt);
		}
		//insert [block#, entry#, cipher, planMap] to ElasticSearch
		esRestClient.bulkInsertDocumentByProcessor(indexName, blockNumber, dataList, cipher,
				1, 1000, 10, ByteSizeUnit.MB, 5);

		return hashTree;
	}

	private void storeHeaderToPBFT(int blockNumber, String root) throws IOException {
		//send [block#, root] to PBFT to PBFT generates Header and store to sqliteDB itself
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		Client client = new Client(prop);

		Operation insertHeaderOp = new InsertHeaderOperation(client.getPublicKey(), blockNumber, root);
		RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), insertHeaderOp);
		client.request(insertRequestMsg);
		int result = (int) client.getReply();
	}

	private Triple<Integer, String, String> getHeader(int blockNumber) throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);

		Operation getHeaderOp = new GetHeaderOperation(client.getPublicKey(), blockNumber);
		RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), getHeaderOp);
		client.request(insertRequestMsg);
		return (Triple<Integer, String, String>) client.getReply();
	}
}
