package kr.ac.hongik.apl.Operations;

import com.owlike.genson.Genson;
import com.owlike.genson.GensonBuilder;
import kr.ac.hongik.apl.ES.EsRestClient;
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
	private EsRestClient esRestClient;

	/**
	 * @param clientInfo   client's PublicKey
	 * @param HttpProtocol HttpProtocol of following query (GET, PUT, POST)
	 * @param SqlQuery     Query String that following ElasticSearch's SQL query format
	 */
	public SQLSearchOperation(PublicKey clientInfo, String HttpProtocol, String SqlQuery) {
		super(clientInfo);
		this.HttpProtocol = HttpProtocol;
		this.SqlQuery = getSqlQuery("query", SqlQuery);
	}

	/**
	 * @param obj Logger.class's instance
	 * @return json format String of response that occur by SQL request
	 */
	@Override
	public String execute(Object obj) {
		try {
			esRestClient = new EsRestClient();
			esRestClient.connectToEs();
			String responseBody = EntityUtils.toString(getResponse(SqlQuery).getEntity());
			String cursorID = getCursorId(responseBody);

			while (cursorID != null) {
				String cursorQuery = getSqlQuery("cursor", cursorID);
				String cursorBody = EntityUtils.toString(getResponse(cursorQuery).getEntity());
				responseBody = concatBody(responseBody, cursorBody);
				cursorID = getCursorId(responseBody);
			}
			esRestClient.disConnectToEs();
			return responseBody;
		} catch (ResponseException e) {
			e.printStackTrace();
			System.err.println("SQLSearchOperation::ResponseException::Continuing PBFT Service...");
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

	private String getCursorId(String body) {
		Genson genson = new GensonBuilder().useClassMetadata(true).useRuntimeType(true).create();
		Map map = genson.deserialize(body, Map.class);
		if (map.containsKey("cursor")) {
			return (String) map.get("cursor");
		} else {
			return null;
		}
	}

	private String concatBody(String to, String from) throws IOException {
		Genson genson = new GensonBuilder().useClassMetadata(true).useRuntimeType(true).create();
		Map toMap = genson.deserialize(to, Map.class);
		Map fromMap = genson.deserialize(from, Map.class);

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
		return genson.serialize(toMap);
	}

	private String getSqlQuery(String key, String query) {
		StringBuilder builder = new StringBuilder();
		return builder.append("{ \"").append(key).append("\" : \"").append(query).append("\"}").toString();
	}
}