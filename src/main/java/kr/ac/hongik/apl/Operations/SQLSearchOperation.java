package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.ES.EsRestClient;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;

import java.io.IOException;
import java.security.PublicKey;

public class SQLSearchOperation extends Operation {
	private String HttpProtocol;
	private String SqlQuery;

	/**
	 * @param clientInfo client's PublicKey
	 * @param HttpProtocol	HttpProtocol of following query (GET, PUT, POST)
	 * @param SqlQuery	Query String that following ElasticSearch's SQL query format
	 */
	public SQLSearchOperation(PublicKey clientInfo, String HttpProtocol, String SqlQuery){
		super(clientInfo);
		this.HttpProtocol = HttpProtocol;
		StringBuilder builder = new StringBuilder();
		this.SqlQuery = builder.append("{ \"query\" : \"").append(SqlQuery).append("\" }").toString();
	}

	/**
	 * @param obj Logger.class's instance
	 * @return	json format String of response that occur by SQL request
	 */
	@Override
	public String execute(Object obj){
		try {
			EsRestClient esRestClient = new EsRestClient();
			esRestClient.connectToEs();

			Request sqlRequest = new Request(HttpProtocol, "_sql/");
			sqlRequest.addParameter("format", "json");
			sqlRequest.setEntity(new NStringEntity(SqlQuery, ContentType.APPLICATION_JSON));

			Response sqlResponse = esRestClient.getClient().getLowLevelClient()
					.performRequest(sqlRequest);

			return EntityUtils.toString(sqlResponse.getEntity());
		} catch (ResponseException e) {
			e.printStackTrace();
			System.err.println("SQLSearchOperation::ResponseException::Continuing PBFT Service...");
			return e.getMessage();
		} catch (NoSuchFieldException | IOException | EsRestClient.EsSSLException e) {
			throw new Error(e);
		}
	}
}