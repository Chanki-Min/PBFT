package kr.ac.hongik.apl.ES;

import com.owlike.genson.Genson;
import kr.ac.hongik.apl.Replica;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.*;
import java.net.URL;
import java.util.*;

public class EsJsonParser {
	String filePath;

	public EsJsonParser() {
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}

	public List<Map> listedJsonFileToList(String key) {
		Genson genson = new Genson();
		InputStream in = EsRestClient.class.getResourceAsStream(filePath);

		Map<String, Object> json = genson.deserialize(in, Map.class);
		return (List<Map>) json.get(key);
	}

	public Map jsonFileToMap() {
		try {
			Genson genson = new Genson();
			InputStream in = EsJsonParser.class.getResourceAsStream(filePath);
			Map<String, Object> map = genson.deserialize(in, Map.class);
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

	public XContentBuilder jsonFileToXContentBuilder(boolean isPrettyPrint) throws IOException {
		Genson genson = new Genson();
		InputStream in = EsRestClient.class.getResourceAsStream(filePath);
		XContentBuilder builder;
		if (isPrettyPrint)
			builder = XContentFactory.jsonBuilder().prettyPrint();
		else
			builder = XContentFactory.jsonBuilder();

		builder.map(genson.deserialize(in, Map.class));
		return builder;
	}

	public Map jsonStringToMap(String json) {
		Genson genson = new Genson();
		return genson.deserialize(json, Map.class);
	}

	public XContentBuilder jsonStringToXContentBuilder(String json, boolean isPrettyPrint) throws IOException {
		Genson genson = new Genson();
		XContentBuilder builder;
		if (isPrettyPrint)
			builder = XContentFactory.jsonBuilder().prettyPrint();
		else
			builder = XContentFactory.jsonBuilder();
		builder.map(genson.deserialize(json, Map.class));
		return builder;
	}

	/**
	 * @param sqlResponse String that result of SQL query that given by SQLSearchOperation
	 * @return LinkedHashMap of sqlResponse (indexNum, [column#1 :row#1-1, ...,column#n :row#1-n])
	 * @throws IOException
	 */
	public LinkedHashMap<Integer, LinkedHashMap> sqlResponseStringToLinkedMap(String sqlResponse) throws IOException {
		Map map = jsonStringToMap(sqlResponse);
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

				Replica.msgDebugger.debug(String.format("Add tmp. block_no: %d entry_no: %d", tmpMap.get("block_number"), tmpMap.get("entry_number")));
			}
			return resultMap;
		} else {
			throw new IOException("sqlResponse param does not has \"columns\", \"rows\"");
		}
	}
}