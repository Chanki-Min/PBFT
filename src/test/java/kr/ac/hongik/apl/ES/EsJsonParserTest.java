package kr.ac.hongik.apl.ES;

import kr.ac.hongik.apl.Util;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
		masterMap.put( "hostName", "223.194.70.105");
		masterMap.put( "port", "19192");
		masterMap.put( "hostScheme", "https");

		esRestClientConfigs.put("masterHostInfo", List.of(masterMap));
	}

	@Test
	public void parseQueryToMapTest() {
		EsJsonParser parser = new EsJsonParser();
		Map info = parser.jsonFileToMap("/Es_testData/Debug_test_data.json");
	}

	@Test
	public void parseIndexMappingToMapTest() throws IOException {
		EsJsonParser mappingParser = new EsJsonParser();
		EsJsonParser settingParser = new EsJsonParser();

		XContentBuilder mappingBuilder;
		XContentBuilder settingBuilder;
		mappingBuilder = mappingParser.jsonFileToXContentBuilder("/ES_MappingAndSetting/Debug_test_mapping.json",true);
		System.out.println(Strings.toString(mappingBuilder));
		settingBuilder = settingParser.jsonFileToXContentBuilder("/ES_MappingAndSetting/Setting.json",true);
		System.out.println(Strings.toString(settingBuilder));
	}

	@Test
	public void plainAndCipherCapacityTest() throws Util.EncryptionException, NoSuchFieldException, EsRestClient.EsSSLException {
		EsRestClient esRestClient = new EsRestClient(esRestClientConfigs);
		esRestClient.connectToEs();
		EsJsonParser parser = new EsJsonParser();
		String seed = "Hello World!";
		SecretKey key = Util.makeSymmetricKey(seed);

		Map originalMap = parser.jsonFileToMap("/Es_testData/Debug_test_data.json");

		String serializedMap = Util.serToBase64String((Serializable) originalMap);
		byte[] enc = Util.encrypt(serializedMap.getBytes(), key);

		String base64EncodedEnc = Base64.getEncoder().encodeToString(enc);
	}
}
