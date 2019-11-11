/*
package kr.ac.hongik.apl.Operations;

import com.owlike.genson.Genson;
import kr.ac.hongik.apl.ES.EsRestClient;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SearchOperation extends Operation {
	private Map queryMap;
	private String[] indices;

	public SearchOperation(PublicKey clientInfo, Map queryMap, String[] indices) {
		super(clientInfo);
		this.queryMap = queryMap;
		this.indices = indices;
	}

	public SearchOperation(PublicKey clientInfo, XContentBuilder queryXContent, String[] indices) {
		super(clientInfo);
		Genson genson = new Genson();
		this.queryMap = genson.deserialize(Strings.toString(queryXContent), Map.class);
		this.indices = indices;
	}

	@Override
	public List<String> execute(Object obj) {
		try {
			QueryBuilder query = QueryBuilders.wrapperQuery(Strings.toString(XContentFactory.jsonBuilder().map(queryMap)));
			final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L)); //expire scrolling after 1Minute
			List<SearchHits> finalResult = new ArrayList<>();
			SearchRequest request = null;
			if (indices == null) {
				request = new SearchRequest();
			} else {
				request = new SearchRequest().indices(indices);
			}
			SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
			searchSourceBuilder.size(10000);
			searchSourceBuilder.query(query);
			request.source(searchSourceBuilder);
			request.scroll(scroll);

			EsRestClient esRestClient = new EsRestClient();
			esRestClient.connectToEs();

			SearchResponse response = esRestClient.getClient().search(request, RequestOptions.DEFAULT);
			String scrollId = response.getScrollId();
			SearchHits hits = response.getHits();
			finalResult.add(hits);

			while (hits != null && hits.getHits().length > 0) {
				SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
				scrollRequest.scroll(scroll);
				response = esRestClient.getClient().scroll(scrollRequest, RequestOptions.DEFAULT);

				scrollId = response.getScrollId();
				hits = response.getHits();
				finalResult.add(hits);
			}
			finalResult.remove(finalResult.size() - 1); //마지막에 스크롤된 빈 원소는 삭제함
			ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
			clearScrollRequest.addScrollId(scrollId);
			esRestClient.getClient().clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
			esRestClient.disConnectToEs();

			return finalResult.stream()
					.map(x -> Strings.toString(x, false, false))
					.collect(Collectors.toList());
		} catch (NoSuchFieldException | IOException | EsRestClient.EsSSLException e) {
			e.printStackTrace();
			throw new Error(e);
		}

	}
}


 */