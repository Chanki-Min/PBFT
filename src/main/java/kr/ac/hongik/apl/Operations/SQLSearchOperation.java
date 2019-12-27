package kr.ac.hongik.apl.Operations;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Replica;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;
import java.security.PublicKey;
import java.util.List;
import java.util.Map;

public class SQLSearchOperation extends Operation {
	private String HttpProtocol;
	private String SqlQuery;
	private int fetchSize;
	private boolean enableAutoPaging ;
	private Map<String, Object> esRestClientConfigs = null;
	private EsRestClient esRestClient = null;
	private ObjectMapper objectMapper = null;

	/**
	 * @param clientInfo   client's PublicKey
	 * @param esRestClientConfigs esRestClient's configs (please look EsRestClient javadoc)
	 * @param HttpProtocol HttpProtocol of following query (GET, PUT, POST)
	 * @param SqlQuery     Query String that following ElasticSearch's SQL query format
	 * @param fetchSize    determine maximum size of one page
	 * @param enableAutoPaging if true, enable auto paging
	 */
	public SQLSearchOperation(PublicKey clientInfo, Map<String, Object> esRestClientConfigs ,String HttpProtocol, String SqlQuery, int fetchSize, boolean enableAutoPaging) {
		super(clientInfo);
		this.esRestClientConfigs = esRestClientConfigs;
		this.SqlQuery = SqlQuery;
		this.HttpProtocol = HttpProtocol;
		this.fetchSize = fetchSize;
		this.enableAutoPaging = enableAutoPaging;

	}

	/**
	 * @param obj Logger.class's instance
	 * @return json format String of response that occur by SQL request
	 */
	@Override
	public String execute(Object obj) {
		try {
			this.objectMapper = new ObjectMapper()
					.enable(JsonParser.Feature.ALLOW_COMMENTS)
					.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
			this.SqlQuery = getSqlQuery("query", SqlQuery, fetchSize);
			esRestClient = new EsRestClient(esRestClientConfigs);
			esRestClient.connectToEs();
			String responseBody = EntityUtils.toString(getResponse(SqlQuery).getEntity());
			if (enableAutoPaging) {
				String cursorID = getCursorId(responseBody);
				while (cursorID != null) {
					String cursorQuery = getSqlQuery("cursor", cursorID, fetchSize);
					String cursorBody = EntityUtils.toString(getResponse(cursorQuery).getEntity());
					responseBody = concatBody(responseBody, cursorBody);
					cursorID = getCursorId(responseBody);
				}
			}
			esRestClient.disConnectToEs();
			return responseBody;
		} catch (ResponseException e) {
			e.printStackTrace();
			Replica.msgDebugger.error(String.format("SQLSearchOperation::ResponseException::Continuing PBFT Service..."));
			return e.getMessage();
		} catch (NoSuchFieldException | IOException | EsRestClient.EsSSLException e) {
			throw new Error(e);
		}
	}

	private Response getResponse(String query) throws IOException, ResponseException {
		Request request = new Request(HttpProtocol, "_sql/");
		request.addParameter("format", "json");
		request.setEntity(new NStringEntity(query, ContentType.APPLICATION_JSON));
		return esRestClient.getClient().getLowLevelClient().performRequest(request);
	}

	private String getCursorId(String body) throws JsonProcessingException {
		Map map = objectMapper.readValue(body, Map.class);
		if (map.containsKey("cursor")) {
			return (String) map.get("cursor");
		} else {
			return null;
		}
	}

	private String concatBody(String to, String from) throws IOException {
		Map toMap = objectMapper.readValue(to, Map.class);
		Map fromMap = objectMapper.readValue(from, Map.class);

		if (toMap.containsKey("rows") && fromMap.containsKey("rows")) {
			((List) toMap.get("rows")).addAll((List) fromMap.get("rows"));
		} else {
			throw new IOException("to, from has wrong format");
		}
		if (fromMap.containsKey("cursor")) {
			toMap.put("cursor", fromMap.get("cursor"));
		} else {
			toMap.remove("cursor");
		}
		return objectMapper.writeValueAsString(toMap);
	}

	//TODO : genson이나 jackson으로 map serialize해서 사용하자
	private String getSqlQuery(String key, String query, int fetchSize) {

		ObjectNode objectNode = objectMapper.createObjectNode();

		objectNode.put(key, query);
		objectNode.put("fetch_size", fetchSize);

		return objectNode.toString();
	}
}