package kr.ac.hongik.apl.ES;

import kr.ac.hongik.apl.Util;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static java.lang.Thread.sleep;

public class EsJsonParserTest {
	private Map<String, Object> esRestClientConfigs;

	@BeforeEach
	public void makeConfig() {
		esRestClientConfigs = new HashMap<>();
		esRestClientConfigs.put("userName", "apl");
		esRestClientConfigs.put("passWord", "wowsan2015@!@#$");
		esRestClientConfigs.put("certPath", "/ES_Connection/esRestClient-cert.p12");
		esRestClientConfigs.put("certPassWord", "wowsan2015@!@#$");

		Map<String, Object> masterMap = new HashMap<>();
		masterMap.put( "name", "es01-master01");
		masterMap.put( "hostName", "223.194.70.111");
		masterMap.put( "port", "51192");
		masterMap.put( "hostScheme", "https");

		esRestClientConfigs.put("masterHostInfo", List.of(masterMap));
	}

	@Test
	public void parseQueryToMapTest() {

		EsJsonParser parser = new EsJsonParser();
		parser.setFilePath("/ES_userData/Debug_test_data.json");
		Map info = parser.jsonFileToMap();
	}

	@Test
	public void parseIndexMappingToMapTest() throws IOException {
		EsJsonParser mappingParser = new EsJsonParser();
		EsJsonParser settingParser = new EsJsonParser();
		mappingParser.setFilePath("/ES_MappingAndSetting/Debug_test_mapping.json");
		settingParser.setFilePath("/ES_MappingAndSetting/Setting.json");

		XContentBuilder mappingBuilder;
		XContentBuilder settingBuilder;
		mappingBuilder = mappingParser.jsonFileToXContentBuilder(true);
		System.out.println(Strings.toString(mappingBuilder));
		settingBuilder = settingParser.jsonFileToXContentBuilder(true);
		System.out.println(Strings.toString(settingBuilder));
	}

	@Test
	public void IndexCreationByParsedJsonTest() throws IOException, NoSuchFieldException, EsRestClient.EsConcurrencyException, EsRestClient.EsException, EsRestClient.EsSSLException {
		String indexName = "es_json_parser_test_index";
		EsJsonParser parser = new EsJsonParser();
		XContentBuilder mappingBuilder;
		XContentBuilder settingBuilder;

		parser.setFilePath("/ES_MappingAndSetting/Debug_test_mapping.json");
		mappingBuilder = parser.jsonFileToXContentBuilder(false);

		parser.setFilePath("/ES_MappingAndSetting/Setting.json");
		settingBuilder = parser.jsonFileToXContentBuilder(false);

		EsRestClient esRestClient = new EsRestClient(esRestClientConfigs);
		esRestClient.connectToEs();
		esRestClient.createIndex(indexName, mappingBuilder, settingBuilder);
		Assertions.assertTrue(esRestClient.isIndexExists(indexName));
		esRestClient.deleteIndex(indexName);
	}

	@Test
	public void insertJsonUserDataParsedToMapTest() throws IOException, EsRestClient.EsConcurrencyException, EsRestClient.EsException, NoSuchFieldException, InterruptedException, EsRestClient.EsSSLException {
		String indexName = "es_json_parser_test_index";
		EsJsonParser parser = new EsJsonParser();
		XContentBuilder mappingBuilder;
		XContentBuilder settingBuilder;

		parser.setFilePath("/ES_MappingAndSetting/Debug_test_mapping.json");
		mappingBuilder = parser.jsonFileToXContentBuilder(false);

		parser.setFilePath("/ES_MappingAndSetting/Setting.json");
		settingBuilder = parser.jsonFileToXContentBuilder(false);

		EsRestClient esRestClient = new EsRestClient(esRestClientConfigs);
		esRestClient.connectToEs();
		esRestClient.createIndex(indexName, mappingBuilder, settingBuilder);

		parser.setFilePath("/ES_userData/Debug_test_data.json");
		List<Map<String, Object>> sampleUserData = new ArrayList<>();
		List<byte[]> encData = new ArrayList<>();

		for (int i = 0; i < 10; i++) {
			sampleUserData.add(parser.jsonFileToMap());
			encData.add(Util.serToString((Serializable) sampleUserData.get(i)).getBytes());
		}


		esRestClient.bulkInsertDocumentByProcessor(
				indexName, 0, sampleUserData, encData, 1, 100, 10, ByteSizeUnit.MB, 5);
		sleep(3000);
		Pair<List<Map<String, Object>>, List<byte[]>> pair = esRestClient.getBlockDataPair(indexName, 0);
	}

	@Test
	public void plainAndCipherCapacityTest() throws Util.EncryptionException, NoSuchFieldException, EsRestClient.EsSSLException {
		EsRestClient esRestClient = new EsRestClient(esRestClientConfigs);
		esRestClient.connectToEs();
		EsJsonParser parser = new EsJsonParser();
		String seed = "Hello World!";
		SecretKey key = Util.makeSymmetricKey(seed);
		parser.setFilePath("/ES_userData/Debug_test_data.json");

		Map originalMap = parser.jsonFileToMap();

		String serializedMap = Util.serToString((Serializable) originalMap);
		byte[] enc = Util.encrypt(serializedMap.getBytes(), key);

		String base64EncodedEnc = Base64.getEncoder().encodeToString(enc);
	}
}
