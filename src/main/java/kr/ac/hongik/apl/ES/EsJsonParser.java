package kr.ac.hongik.apl.ES;

import com.owlike.genson.Genson;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.*;
import java.net.URL;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EsJsonParser {
	String filePath;

	public EsJsonParser() { }

	public void setFilePath(String filePath) { this.filePath = filePath; }

	public List<Map> listedJsonToList(String key){
		Genson genson = new Genson();
		InputStream in = EsRestClient.class.getResourceAsStream(filePath);

		Map<String, Object> json = genson.deserialize(in, Map.class);
		return (List<Map>) json.get(key);
	}

	public Map jsonToMap(){
		try {

		Genson genson = new Genson();
		InputStream in = EsJsonParser.class.getResourceAsStream(filePath);
		Map<String, Object> map = genson.deserialize(in, Map.class);
		for(var ent : map.entrySet()){
			if(ent.getValue() instanceof String && ((String) ent.getValue()).startsWith("BINARY_PATH::")) {
				Base64.Encoder encoder = Base64.getEncoder();
				String resourcePath = ((String) ent.getValue()).replace("BINARY_PATH::","");
				URL fileURL = EsJsonParser.class.getResource(resourcePath);

				File file = new File(fileURL.getPath());
				if(file.exists()) {
					FileInputStream fin = new FileInputStream(file);
					ByteArrayOutputStream bao = new ByteArrayOutputStream();
					int len = 0;
					byte[] buf = new byte[1024];
					while( (len = fin.read( buf )) != -1 ) {
						bao.write(buf, 0, len);
					}
					String encodeValue = Base64.getEncoder().encodeToString(bao.toByteArray());
					map.put(ent.getKey(), encodeValue);
				}
			}
		}
			return map;
		}catch (IOException e){
			e.printStackTrace();
			return null;
		}
	}

	public XContentBuilder jsonToXcontentBuilder(boolean isPrettyPrint) throws IOException{
		Genson genson = new Genson();
		InputStream in = EsRestClient.class.getResourceAsStream(filePath);
		XContentBuilder builder;
		if(isPrettyPrint)
			builder = XContentFactory.jsonBuilder().prettyPrint();
		else
			builder = XContentFactory.jsonBuilder();

		builder.map(genson.deserialize(in, Map.class));
		return builder;
	}

	public List<String> getKeyListFromJsonMapping() {
		Genson genson = new Genson();
		InputStream in = EsRestClient.class.getResourceAsStream(filePath);

		Map mapping = genson.deserialize(in, Map.class);
		Map properties = (Map) mapping.get("properties");
		return (List<String>) properties.keySet().stream().collect(Collectors.toList());
	}
}
