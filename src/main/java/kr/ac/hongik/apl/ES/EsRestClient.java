package kr.ac.hongik.apl.ES;



import com.owlike.genson.Genson;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.*;

public class EsRestClient {

	private final int BULKBUFFER = 500;
	private List<String> hostNames = new ArrayList<>();
	private List<Integer> ports = new ArrayList<>();
	private List<String> hostSchemes = new ArrayList<>();
	private RestHighLevelClient restHighLevelClient = null;

	public EsRestClient(){
	}

	public void connectToEs() throws NoSuchFieldException{
		getMasterNodeInfo();
		List<HttpHost> httpHosts = new ArrayList<>();
		for (int i = 0; i < hostNames.size(); i++) {
			httpHosts.add(new HttpHost(hostNames.get(i), ports.get(i), hostSchemes.get(i)));
		}
		HttpHost[] httpHostsArr = httpHosts.toArray(new HttpHost[0]);
		restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHostsArr));
	}

	public void disConnectToEs() throws IOException{
		restHighLevelClient.close();
	}

	public RestHighLevelClient getClient(){
		return restHighLevelClient;
	}

	public final ClusterHealthResponse getClusterInfo() throws IOException{
		ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest();
		ClusterHealthResponse response = restHighLevelClient.cluster().health(clusterHealthRequest, RequestOptions.DEFAULT);
		return response;
	}

	public void deleteIndex(String indexName) throws IOException, SQLException{
		boolean isIndexExists = this.isIndexExists(indexName);
		if(!isIndexExists){
			throw new SQLException(indexName+ " does not exists");
		}


		DeleteIndexRequest request = new DeleteIndexRequest(indexName);
		AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
		if(response.isAcknowledged()){
			System.out.println(indexName + " DELETED from Cluster");
		}else{
			throw new SQLException("Cannot DELETE "+indexName);
		}
	}

	public void createIndex(String indexName , XContentBuilder mapping, XContentBuilder setting) throws IOException, SQLException{


		boolean isIndexExists = this.isIndexExists(indexName);
		if(isIndexExists){
			throw new SQLException(indexName+ " is Already exists");
		}
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		request.mapping(mapping);
		request.settings(setting);

		CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
		if(response.isAcknowledged()){
			System.out.println(indexName + " CREATED to Cluster");
		}else{
			throw new SQLException("Cannot CREATE "+indexName);
		}
	}

	public void bulkInsertDocument(String indexName, int blockNumber, List<byte[]> encDatas, int versionNumber) throws IOException, SQLException{
		Base64.Encoder encoder = Base64.getEncoder();
		Boolean[] checkList = new Boolean[3];
		checkList[0] = this.isIndexExists(indexName);
		checkList[1] = !this.isBlockExists(indexName,blockNumber);
		checkList[2] = blockNumber == (getMaximumBlockNumber(indexName) + 1);

		if(Arrays.stream(checkList).anyMatch(x -> !x)){
			throw new ElasticsearchException("Request does not fulfil checkList");
		}

		BulkRequest request = new BulkRequest();
		for(int entryNumber = 0; entryNumber<encDatas.size(); entryNumber++) {

			String id = idGenerator(indexName,blockNumber,entryNumber);
			String base64EncodedData = encoder.encodeToString(encDatas.get(entryNumber));
			XContentBuilder builder = new XContentFactory().jsonBuilder();
			builder.startObject();
			{
				builder.field("block_number", blockNumber);
				builder.field("entry_number", entryNumber);
				builder.field("encrypt_data", base64EncodedData);
			}
			builder.endObject();
			request.add(new IndexRequest(indexName).id(id).source(builder).version(versionNumber).versionType(VersionType.EXTERNAL));

			//if Bulk request's size equals to BULKBUFFER size. execute request
			if(entryNumber%BULKBUFFER == BULKBUFFER-1){
				BulkResponse responses = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
				request = new BulkRequest();
				System.out.println("BULK_CREATED, status: "+responses.status());
				System.out.println(responses.buildFailureMessage());
				System.out.println("Request sent | Cause: BULKBUFFER EXCEED | Entry Number: "+entryNumber);
				if(responses.hasFailures()){
					throw new SQLException(responses.buildFailureMessage());
				}
			}

		}
		//if Bulk request has left IndexRequest, execute last of them
		if(request.requests().size() != 0){
			BulkResponse responses = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
			System.out.println("Created, status: "+responses.status());
			System.out.println(responses.buildFailureMessage());
			System.out.println("Request sent | Cause: This is Last buffer |");
			if(responses.hasFailures()){
				throw new SQLException(responses.buildFailureMessage());
			}
		}

	}

	public List<byte[]> getBlockByteArray(String indexName, int blockNumber) throws IOException, SQLException{

		if(!isIndexExists(indexName)){
			throw new SQLException(indexName+ " does not exists");
		}
		if(!isBlockExists(indexName,blockNumber)){
			throw new SQLException(blockNumber+ " does not exists in "+indexName);
		}

		Base64.Decoder decoder = Base64.getDecoder();
		SearchRequest request = new SearchRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.matchQuery("block_number", blockNumber));
		builder.from(0);
		builder.size(getBlockEntrySize(indexName, blockNumber));		//set search range to [0, index Size]

		request.source(builder);
		SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
		SearchHit[] searchHits = response.getHits().getHits();

		List<byte[]> restoredBytes = new ArrayList<>();

		for(SearchHit searchHit : searchHits){
			restoredBytes.add(decoder.decode( (String) searchHit.getSourceAsMap().get( "encrypt_data") ) );
		}
		return restoredBytes;
	}

	public byte[] getBlockEntryByteArray(String indexName, int blockNumber, int entryNumber) throws IOException, SQLException{

		if(!isIndexExists(indexName)){
			throw new SQLException(indexName+ " does not exists");
		}
		if(!isBlockExists(indexName,blockNumber)){
			throw new SQLException(blockNumber+ " does not exists in "+indexName);
		}
		if(!isEntryExists(indexName, blockNumber, entryNumber)){
			throw new SQLException(entryNumber+ " does not exists in block "+blockNumber);
		}

		Base64.Decoder decoder = Base64.getDecoder();
		SearchRequest request = new SearchRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.termQuery("block_number", blockNumber));
		builder.query(QueryBuilders.termQuery("entry_number", entryNumber));
		request.source(builder);

		SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
		SearchHit[] searchHits = response.getHits().getHits();

		byte[] restoredByte;
		restoredByte = decoder.decode((String) searchHits[0].getSourceAsMap().get("encrypt_data"));

		return restoredByte;
	}

	private void getMasterNodeInfo() throws NoSuchFieldException{
		try {
			Genson genson = new Genson();
			InputStream in = EsRestClient.class.getResourceAsStream("/master.json");

			Map<String, Object> json = genson.deserialize(in, Map.class);
			ArrayList<HashMap> masterMap = (ArrayList<HashMap>) json.get("masterHostInfo");

			Boolean[] checkList = new Boolean[3];

			for(var masterInfo : masterMap) {
				this.hostNames.add(masterInfo.get("hostName").toString());
				this.ports.add(Integer.parseInt(masterInfo.get("port").toString()));
				this.hostSchemes.add(masterInfo.get("hostScheme").toString());
			}
			checkList[0] = hostNames.stream().distinct().count() == masterMap.size();
			checkList[1] = ports.size() == masterMap.size();
			checkList[2] = hostSchemes.size() == masterMap.size();

			if(Arrays.stream(checkList).allMatch(x -> x))
			{
				return;
			}else{
				System.err.println("master.json has unexpected format");
				throw new NoSuchFieldException();
			}
		}catch (Exception e){
			StringBuilder builder = new StringBuilder();
			throw new NoSuchFieldException();
		}
	}

	public boolean isIndexExists(String indexName) throws IOException{
		GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
		return restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
	}

	private boolean isBlockExists(String indexName, int blockNumber) throws IOException{
		CountRequest getCorrupedCount = new CountRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.termQuery("block_number", blockNumber));
		getCorrupedCount.source(builder);

		CountResponse response = restHighLevelClient.count(getCorrupedCount, RequestOptions.DEFAULT);
		return response.getCount() != 0;
	}

	private boolean isEntryExists(String indexName, int blockNumber, int entryNumber) throws IOException{
		CountRequest getCorrupedCount = new CountRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.termQuery("block_number", blockNumber));
		builder.query(QueryBuilders.termQuery("entry_number", entryNumber));
		getCorrupedCount.source(builder);

		CountResponse response = restHighLevelClient.count(getCorrupedCount, RequestOptions.DEFAULT);
		return response.getCount() != 0;
	}

	private int getMaximumBlockNumber(String indexName) throws IOException{



		SearchRequest searchMaxRequest = new SearchRequest(indexName);
		SearchSourceBuilder maxBuilder = new SearchSourceBuilder();
		maxBuilder.query(QueryBuilders.matchAllQuery());
		MaxAggregationBuilder aggregation =
				AggregationBuilders.max("maxValueAgg").field("block_number");
		maxBuilder.aggregation(aggregation);
		searchMaxRequest.source(maxBuilder);
		SearchResponse response = restHighLevelClient.search(searchMaxRequest, RequestOptions.DEFAULT);

		if(response.getHits().getTotalHits().value == 0){
			return -1;
		}


		ParsedMax maxValue = response.getAggregations().get("maxValueAgg");	//get max_aggregation from response
		return (int) maxValue.getValue();
	}

	private int getBlockEntrySize(String indexName, int blockNumber) throws IOException{
		CountRequest request = new CountRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.matchQuery("block_number", blockNumber));
		CountResponse response = restHighLevelClient.count(request, RequestOptions.DEFAULT);
		return (int) response.getCount();
	}

	private String idGenerator(String indexName,int blockNumber, int entryNumber){
		StringBuilder builder = new StringBuilder();
		builder.append(indexName).append("_").append(blockNumber).append("_").append(entryNumber);
		return String.valueOf(builder);
	}

}


