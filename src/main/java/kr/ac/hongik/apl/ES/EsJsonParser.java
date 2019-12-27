package kr.ac.hongik.apl.ES;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import kr.ac.hongik.apl.Replica;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.*;
import java.net.URL;
import java.util.*;

public class EsJsonParser {
	ObjectMapper objectMapper = new ObjectMapper()
			.enable(JsonParser.Feature.ALLOW_COMMENTS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

	public EsJsonParser() {
	}

	public Map jsonFileToMap(String filePath) {
		try {
			InputStream in = EsJsonParser.class.getResourceAsStream(filePath);
			Map<String, Object> map = objectMapper.readValue(in, Map.class);
			for (var ent: map.entrySet()) {
				if (ent.getValue() instanceof String && ((String) ent.getValue()).startsWith("BINARY_PATH::")) {
					Base64.Encoder encoder = Base64.getEncoder();
					String resourcePath = ((String) ent.getValue()).replace("BINARY_PATH::", "");
					URL fileURL = EsJsonParser.class.getResource(resourcePath);
					File file = new File(fileURL.getPath());
					if (file.exists()) {
						FileInputStream fin = new FileInputStream(file);
						ByteArrayOutputStream bao = new ByteArrayOutputStream();
						int len = 0;
						byte[] buf = new byte[1024];
						while ((len = fin.read(buf)) != -1) {
							bao.write(buf, 0, len);
						}
						String encodeValue = Base64.getEncoder().encodeToString(bao.toByteArray());
						map.put(ent.getKey(), encodeValue);
					}
				}
			}
			return map;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public XContentBuilder jsonFileToXContentBuilder(String filePath, boolean isPrettyPrint) throws IOException {
		InputStream in = EsRestClient.class.getResourceAsStream(filePath);
		XContentBuilder builder;
		if (isPrettyPrint)
			builder = XContentFactory.jsonBuilder().prettyPrint();
		else
			builder = XContentFactory.jsonBuilder();

		builder.map(objectMapper.readValue(in, Map.class));
		return builder;
	}

	public XContentBuilder jsonStringToXContentBuilder(String json, boolean isPrettyPrint) throws IOException {
		XContentBuilder builder;
		if (isPrettyPrint)
			builder = XContentFactory.jsonBuilder().prettyPrint();
		else
			builder = XContentFactory.jsonBuilder();
		builder.map(objectMapper.readValue(json, Map.class));
		return builder;
	}

	/**
	 * @param sqlResponse String that result of SQL query that given by SQLSearchOperation
	 * @return LinkedHashMap of sqlResponse (indexNum, [column#1 :row#1-1, ...,column#n :row#1-n])
	 * @throws IOException
	 */
	public LinkedHashMap<Integer, LinkedHashMap> sqlResponseStringToLinkedMap(String sqlResponse) throws IOException {
		Map map = objectMapper.readValue(sqlResponse, Map.class);
		if (map.containsKey("columns") && map.containsKey("rows")) {
			List<HashMap> columns = (List) (map.get("columns"));
			List<ArrayList> rows = (List) map.get("rows");

			LinkedHashMap resultMap = new LinkedHashMap();
			int count = 0;
			for (int i = 0; i < rows.size(); i++) {
				LinkedHashMap tmpMap = new LinkedHashMap();
				for (int j = 0; j < columns.size(); j++) {
					tmpMap.put(columns.get(j).get("name"), rows.get(i).get(j));
				}
				resultMap.put(i, tmpMap);
				count++;

				Replica.msgDebugger.debug(String.format("Add tmp. block_no: %s entry_no: %s", tmpMap.get("block_number"), tmpMap.get("entry_number")));
			}
			return resultMap;
		} else {
			throw new IOException("sqlResponse param does not has \"columns\", \"rows\"");
		}
	}
}