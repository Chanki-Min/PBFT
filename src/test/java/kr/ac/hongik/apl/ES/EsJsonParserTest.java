package kr.ac.hongik.apl.ES;

import kr.ac.hongik.apl.Util;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;

public class EsJsonParserTest {
	@Test
	public void MasterInfoJsonParsingTest(){
		List<String> keyList = new ArrayList<>();
		keyList.add("masterHostInfo");

		EsJsonParser parser = new EsJsonParser();
		parser.setFilePath("/ES_MappingAndSetting/master.json");
		List<Map> masters = parser.listedJsonToList("masterHostInfo");
	}

	@Test
	public void parseQueryToMapTest(){
		List<String> keyList = new ArrayList<>();
		keyList.add("real_name");
		keyList.add("user_name");
		keyList.add("start_time");
		keyList.add("end_time");
		keyList.add("start_location");
		keyList.add("end_location");

		EsJsonParser parser = new EsJsonParser();
		parser.setFilePath("/ES_MappingAndSetting/sample_one_userInfo.json");
		Map info = parser.jsonToMap();
	}

	@Test
	public void parseIndexMappingToMapTest() throws IOException{
		EsJsonParser mappingParser = new EsJsonParser();
		EsJsonParser settingParser = new EsJsonParser();
		mappingParser.setFilePath("/ES_MappingAndSetting/ES_mapping_with_plain.json");
		settingParser.setFilePath("/ES_MappingAndSetting/ES_setting_with_plain.json");

		XContentBuilder mappingBuilder;
		XContentBuilder settingBuilder;
		mappingBuilder = mappingParser.jsonToXcontentBuilder(true);
		System.out.println(Strings.toString(mappingBuilder));
		settingBuilder = settingParser.jsonToXcontentBuilder(true);
		System.out.println(Strings.toString(settingBuilder));
	}

	@Test
	public void IndexCreationByParsedJsonTest() throws IOException, NoSuchFieldException, EsRestClient.EsConcurrencyException, EsRestClient.EsException{
		String indexName = "es_json_parser_test_index";
		EsJsonParser parser = new EsJsonParser();
		XContentBuilder mappingBuilder;
		XContentBuilder settingBuilder;

		parser.setFilePath("/ES_MappingAndSetting/ES_mapping_with_plain.json");
		mappingBuilder = parser.jsonToXcontentBuilder(false);

		parser.setFilePath("/ES_MappingAndSetting/ES_setting_with_plain.json");
		settingBuilder = parser.jsonToXcontentBuilder(false);

		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();
		esRestClient.createIndex(indexName, mappingBuilder, settingBuilder);
		Assertions.assertTrue(esRestClient.isIndexExists(indexName));
		esRestClient.deleteIndex(indexName);
	}

	@Test
	public void insertJsonUserDataParsedToMapTest() throws IOException, EsRestClient.EsConcurrencyException, EsRestClient.EsException, NoSuchFieldException, InterruptedException{
		String indexName = "es_json_parser_test_index";
		EsJsonParser parser = new EsJsonParser();
		XContentBuilder mappingBuilder;
		XContentBuilder settingBuilder;

		parser.setFilePath("/ES_MappingAndSetting/ES_mapping_with_plain.json");
		mappingBuilder = parser.jsonToXcontentBuilder(false);

		parser.setFilePath("/ES_MappingAndSetting/ES_setting_with_plain.json");
		settingBuilder = parser.jsonToXcontentBuilder(false);

		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();
		esRestClient.createIndex(indexName, mappingBuilder, settingBuilder);

		parser.setFilePath("/ES_MappingAndSetting/sample_one_userInfo.json");
		List<Map<String, Object>> sampleUserData = new ArrayList<>();
		List<byte[]> encData = new ArrayList<>();

		for(int i=0; i<10; i++) {
			sampleUserData.add(parser.jsonToMap());
			encData.add(Util.serToString((Serializable) sampleUserData.get(i)).getBytes());
		}


		esRestClient.bulkInsertDocument(indexName, 0, sampleUserData, encData, 1);
		sleep(3000);
		Pair<List<Map<String, Object>>, List<byte[]>> pair = esRestClient.getBlockDataPair(indexName,0);
	}
}
