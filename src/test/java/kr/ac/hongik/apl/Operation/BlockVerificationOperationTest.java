package kr.ac.hongik.apl.Operation;

import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.Blockchain.HashTree;
import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.ES.EsJsonParser;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Generator.Generator;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.BlockVerificationOperation;
import kr.ac.hongik.apl.Operations.InsertHeaderOperation;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Util;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
		esRestClient.close();
	}

	@Test
	public void makeBlockAndVerifyBlockTest() throws Exception {
		String dummyDataPath = "/Es_testData/Init_data00.json";
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

		List<String> hashList = new ArrayList<>();
		for (Map<String, Object> carLogMap: carLogList) {
			String s = objectMapper.writeValueAsString(carLogMap);
			String hash = Util.hash(s);
			hashList.add(hash);
		}
		HashTree hashTree = new HashTree(hashList.toArray(new String[0]));
		int blockNumber = storeHeaderAndHashToPBFTAndReturnIdx(hashTree.toString(), hashList);
		storeDataToEs(indexName, blockNumber, carLogList);
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

	private void storeDataToEs(String indexName, int blockNumber, List<Map<String, Object>> dataList) throws IOException, EsRestClient.EsConcurrencyException, InterruptedException, EsRestClient.EsException {
		esRestClient.bulkInsertDocumentByProcessor(indexName, blockNumber, dataList,
				1, 1000, 10, ByteSizeUnit.MB, 5);
	}

	private int storeHeaderAndHashToPBFTAndReturnIdx(String root, List<String> hashList) throws IOException {
		//send [block#, root] to PBFT to PBFT generates Header and store to sqliteDB itself
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		Client client = new Client(prop);

		Operation insertHeaderOp = new InsertHeaderOperation(client.getPublicKey(), hashList, root);
		RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), insertHeaderOp);
		client.request(insertRequestMsg);
		int blockNumber = (int) client.getReply();
		return blockNumber;
	}
}
