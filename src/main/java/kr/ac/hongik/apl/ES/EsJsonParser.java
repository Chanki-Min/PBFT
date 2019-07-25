package kr.ac.hongik.apl.ES;

import com.owlike.genson.Genson;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.InputStream;
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
		Genson genson = new Genson();
		InputStream in = EsRestClient.class.getResourceAsStream(filePath);

		return genson.deserialize(in, Map.class);
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
