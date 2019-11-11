package kr.ac.hongik.apl.Operations;


import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Replica;
import org.apache.http.HttpResponse;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.lang.reflect.Field;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SearchOperation extends Operation {
	private Map<String, Object> esRestClientConfigs;
	private String HttpProtocol;
	private String endpoint;
	private Map<String, String> parameterMap;
	private String body;

	private EsRestClient esRestClient;

	public SearchOperation(PublicKey clientInfo, Map<String, Object> esConfig, String HttpProtocol,
						   String endpoint, Map parameterMap, String body) {
		super(clientInfo);
		this.esRestClientConfigs = esConfig;
		this.HttpProtocol = HttpProtocol;
		this.endpoint = endpoint;
		this.parameterMap = parameterMap;
		this.body = body;
	}

	/**
	 * @param obj logger
	 * @return	http entity of elasticsearch response
	 * @throws SQLException
	 */
	@Override
	public Object execute(Object obj) throws SQLException {
		try {
			Pattern searchPatten = Pattern.compile(".*_search.*|.*_sql.*");
			Matcher matcher = searchPatten.matcher(endpoint);
			if(!matcher.matches()) {
				throw new IllegalArgumentException(String.format("endpoint [%s] does not have _search or _sql parameter.", endpoint));
			}

			Logger logger = (Logger) obj;
			esRestClient = new EsRestClient(esRestClientConfigs);
			esRestClient.connectToEs();

			Request request = new Request(HttpProtocol, endpoint);
			parameterMap.entrySet().stream().
					forEach(e -> request.addParameter(e.getKey(), e.getValue()));
			request.setEntity(new NStringEntity(body, org.apache.http.entity.ContentType.APPLICATION_JSON));
			Response response = esRestClient.getClient().getLowLevelClient().performRequest(request);

			Field field = response.getClass().getDeclaredField("response");
			field.setAccessible(true);
			HttpResponse httpResponse = (HttpResponse) field.get(response);

			return getHttpMap(httpResponse, null);
		} catch (EsRestClient.EsSSLException | NoSuchFieldException | IOException | IllegalArgumentException | IllegalAccessException e) {
			Replica.msgDebugger.error(e.getMessage());
			LinkedHashMap<String, String> httpMap = new LinkedHashMap<>();
			return getHttpMap(null, e);
		}
	}

	private LinkedHashMap<String, String> getHttpMap(HttpResponse httpResponse, Exception e) {
		try {
			LinkedHashMap<String, String> httpMap = new LinkedHashMap<>();
			if (httpResponse != null && e == null) {
				httpMap.put("protocolVersion", httpResponse.getStatusLine().getProtocolVersion().toString());
				httpMap.put("statusCode", Integer.toString(httpResponse.getStatusLine().getStatusCode()));
				httpMap.put("reasonPhrase", httpResponse.getStatusLine().getReasonPhrase());
				httpMap.put("entity", EntityUtils.toString(httpResponse.getEntity()));
				httpMap.put("locale", httpResponse.getLocale().toString());
				httpMap.put("error", null);
			} else if (httpResponse == null && e != null) {
				httpMap.put("protocolVersion", null);
				httpMap.put("statusCode", null);
				httpMap.put("reasonPhrase", null);
				httpMap.put("entity", null);
				httpMap.put("locale", null);
				httpMap.put("error", e.getMessage());
				return httpMap;
			}
			return httpMap;
		} catch (IOException ex) {
			Replica.msgDebugger.error(ex.getMessage());
			throw new Error(ex);
		}
	}
}
