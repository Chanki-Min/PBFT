package kr.ac.hongik.apl.ES;


import com.owlike.genson.Genson;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchStatusException;
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
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class EsRestClient {

	private List<String> hostNames = new ArrayList<>();
	private List<Integer> ports = new ArrayList<>();
	private List<String> hostSchemes = new ArrayList<>();
	private RestHighLevelClient restHighLevelClient = null;
	private final boolean DEBUG = false;

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

	public void deleteIndex(String indexName) throws IOException, EsException{
		boolean isIndexExists = this.isIndexExists(indexName);
		if(!isIndexExists){
			throw new EsException(indexName+ " does not exists");
		}


		DeleteIndexRequest request = new DeleteIndexRequest(indexName);
		AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
		if(response.isAcknowledged()){
			System.out.println(indexName + " DELETED from Cluster");
		}else{
			throw new EsException("Cannot DELETE "+indexName);
		}
	}

	/**
	 * @param indexName
	 * @throws IOException
	 * @throws EsException	throws when creadtion failed
	 * @throws EsConcurrencyException throws when index already created by other replicas
	 */
	public void createIndex(String indexName , XContentBuilder mapping, XContentBuilder setting) throws IOException, EsException, EsConcurrencyException{


		boolean isIndexExists = this.isIndexExists(indexName);
		if(isIndexExists){
			throw new EsException(indexName+ " is Already exists");
		}
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		request.mapping(mapping);
		request.settings(setting);

		try {
			CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
			if(response.isAcknowledged()){
				System.out.println(indexName + " CREATED to Cluster");
			}else{
				throw new EsException("Cannot CREATE "+indexName);
			}
		}catch (ElasticsearchStatusException e){
			throw new EsConcurrencyException(e.getMessage());
		}
	}

	/**
	 * @param indexName
	 * @throws IOException
	 * @throws EsException	throws when creadtion failed
	 * @throws EsConcurrencyException throws when index already created by other replicas
	 */
	public void createIndex(String indexName) throws IOException, EsException, EsConcurrencyException{

		if (!isIndexExists(indexName)) {

			XContentBuilder mappingBuilder = new XContentFactory().jsonBuilder();
			mappingBuilder.startObject();
			{
				mappingBuilder.startObject("properties");
				{
					mappingBuilder.startObject("block_number");
					{
						mappingBuilder.field("type", "long");
						mappingBuilder.field("coerce", false);            //forbid auto-casting String to Integer
						mappingBuilder.field("ignore_malformed", true);    //forbid non-numeric values
					}
					mappingBuilder.endObject();

					mappingBuilder.startObject("entry_number");
					{
						mappingBuilder.field("type", "long");
						mappingBuilder.field("coerce", false);
						mappingBuilder.field("ignore_malformed", true);
					}
					mappingBuilder.endObject();

					mappingBuilder.startObject("encrypt_data");
					{
						mappingBuilder.field("type", "binary");
					}
					mappingBuilder.endObject();
				}
				mappingBuilder.endObject();
				mappingBuilder.field("dynamic", "strict");    //forbid auto field creation
			}
			mappingBuilder.endObject();
			XContentBuilder settingsBuilder = new XContentFactory().jsonBuilder();
			settingsBuilder.startObject();
			{
				settingsBuilder.field("index.number_of_shards", 4);
				settingsBuilder.field("index.number_of_replicas", 3);
				settingsBuilder.field("index.merge.scheduler.max_thread_count", 1);
			}
			settingsBuilder.endObject();

			CreateIndexRequest request = new CreateIndexRequest(indexName);
			request.mapping(mappingBuilder);
			request.settings(settingsBuilder);

			try {
				CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
				if(response.isAcknowledged()){
					System.out.println(indexName + " CREATED to Cluster");
				}else{
					throw new EsException("Cannot CREATE "+indexName);
				}
			}catch (ElasticsearchStatusException e){
				throw new EsConcurrencyException(e.getMessage());
			}

		}
	}

	/**
	 * @param indexName
	 * @param blockNumber
	 * @param encDatas
	 * @param versionNumber number that stating with "1" and MUST increases whenever document updates
	 * @return	ElasticSearch's BulkResponse instance of  data-insertion
	 * @throws IOException
	 * @throws EsException throws when (index not exists, some of insertion failed)
	 * @throws EsConcurrencyException
	 * throws when headDocument of (indexName,BlockNumber) already exists.
	 * that means other replica already bulkInserting to certain version, so cancel method
 	 */
	public BulkResponse bulkInsertDocument(String indexName, int blockNumber, List<byte[]> encDatas, long versionNumber) throws IOException, EsException, EsConcurrencyException{
		if(versionNumber <= getDocumentVersion(indexName,blockNumber,-1))
			throw new EsConcurrencyException("current version is higher than this");

		BulkResponse bulkResponse = null;
		final int BULKBUFFER;
		if(DEBUG)
			BULKBUFFER = 500;
		else
			BULKBUFFER = encDatas.size() + 1;

		if(!this.isIndexExists(indexName))
			throw new EsException("index :"+indexName+" does not exists");

		//insert HeadDocument, when Head already exist for (indexName,blockNumber,versionNumber), throw exception and cancel bulkInsertion
		try {
			restHighLevelClient.index(getHeadDocument(indexName,blockNumber,versionNumber), RequestOptions.DEFAULT);
		}catch (ElasticsearchStatusException e) {
			StringBuilder builder = new StringBuilder();
			builder.append(this.getClass().getName()).append("::bulkInsertDocument").append(" ConcurrencyException");
			builder.append(" indexName :").append(indexName).append(" BlockNum :").append(blockNumber);
			builder.append(" Cause :Document inserting already executing by other replica");
			throw new EsConcurrencyException(builder.toString());
		}


		Base64.Encoder encoder = Base64.getEncoder();
		BulkRequest request = new BulkRequest();
		request.setRefreshPolicy("wait_for");	//do not refresh (searchable) index until bulk request finish
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
				bulkResponse = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
				request = new BulkRequest();
				System.out.println("BULK_CREATED, status: "+bulkResponse.status());
				System.out.println(bulkResponse.buildFailureMessage());
				System.out.println("Request sent | Cause: BULKBUFFER EXCEED | Entry Number: "+entryNumber);
				if(bulkResponse.hasFailures()){
					throw new EsException(bulkResponse.buildFailureMessage());
				}
			}

		}
		//if Bulk request has left IndexRequest, execute last of them
		if(request.requests().size() != 0){
			bulkResponse = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
			System.out.println("Created, status: "+bulkResponse.status());
			System.out.println(bulkResponse.buildFailureMessage());
			System.out.println("Request sent | Cause: This is Last buffer |");
		}
		return bulkResponse;
	}

	/**
	 * @param indexName
	 * @param blockNumber
	 * @return encrypted_data in #blockNumber th block EXCEPT headDocument
	 * @throws IOException
	 * @throws EsException
	 */
	public List<byte[]> getBlockByteArray(String indexName, int blockNumber) throws IOException, EsException{

		if(!isIndexExists(indexName)){
			throw new EsException(indexName+ " does not exists");
		}
		if(!isBlockExists(indexName,blockNumber)){
			throw new EsException(blockNumber+ " does not exists in "+indexName);
		}
		Base64.Decoder decoder = Base64.getDecoder();
		SearchRequest request = new SearchRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.matchQuery("block_number", blockNumber));
		builder.query(QueryBuilders.boolQuery().mustNot(QueryBuilders.matchQuery("_id", idGenerator(indexName,blockNumber,-1))));	//do not search headDocument

		builder.sort("entry_number", SortOrder.ASC);	//set sort option to "entry_number" ascending

		builder.from(0);
		builder.size(getBlockEntrySize(indexName, blockNumber) -1);		//set search range to [0, index Size]


		request.source(builder);
		SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
		SearchHit[] searchHits = response.getHits().getHits();
		List<byte[]> restoredBytes = new ArrayList<>();

		for(SearchHit searchHit : searchHits){
			restoredBytes.add(decoder.decode( (String) searchHit.getSourceAsMap().get( "encrypt_data") ) );
		}
		return restoredBytes;
	}

	public byte[] getBlockEntryByteArray(String indexName, int blockNumber, int entryNumber) throws IOException, EsException{

		if(!isIndexExists(indexName)){
			throw new EsException(indexName+ " does not exists");
		}
		if(!isBlockExists(indexName,blockNumber)){
			throw new EsException(blockNumber+ " does not exists in "+indexName);
		}
		if(!isEntryExists(indexName, blockNumber, entryNumber)){
			throw new EsException(entryNumber+ " does not exists in block "+blockNumber);
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

	/**
	 * read "resources/master.json" and parse to httpHost format
	 * @throws NoSuchFieldException
	 */
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

	public boolean isDataEquals(List<byte[]> arr1, List<byte[]> arr2){
		if(arr1.size() != arr2.size())
			return false;
		Boolean[] checkList = new Boolean[arr1.size()];

		for(int i=0; i<arr1.size(); i++){
			checkList[i] = Arrays.equals(arr1.get(i), arr2.get(i));
		}
		return Arrays.stream(checkList).allMatch(Boolean::booleanValue);
	}

	private int getRightNextBlockNumber(String indexName) throws IOException{



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

	/**
	 * @param indexName
	 * @param blockNumber
	 * @param entryNumber
	 * @return	String that format : indexName_blockNumber_entryNumber
	 */
	private String idGenerator(String indexName,int blockNumber, int entryNumber){
		StringBuilder builder = new StringBuilder();
		builder.append(indexName).append("_").append(blockNumber).append("_").append(entryNumber);
		return String.valueOf(builder);
	}

	/**
	 * @param indexName
	 * @param blockNumber
	 * @param versionNumber
	 * @return	IndexRequest of indexing headDocument by given params
	 * <pre>{@code
	 * PUT indexName/_doc/indexName_blockNumber_-1?version=versionNumber&version_type="external"
	 * {
	 *	"block_number", blockNumber,
	 *	"entry_number", -1
	 *	"encrypt_data", ""
	 * }
	 * }</pre>
	 * @throws IOException
	 */
	private IndexRequest getHeadDocument(String indexName, int blockNumber, long versionNumber) throws IOException{
		XContentBuilder builder = new XContentFactory().jsonBuilder();
		builder.startObject();
		{
			builder.field("block_number", blockNumber);
			builder.field("entry_number", -1);
			builder.field("encrypt_data", "");
		}
		builder.endObject();

		return new IndexRequest(indexName).id(idGenerator(indexName,blockNumber,-1))
				.source(builder).version(versionNumber).versionType(VersionType.EXTERNAL);
	}

	public long getDocumentVersion(String indexName, int blockNumber, int entryNumber) throws IOException{
		long version;
		String id = idGenerator(indexName,blockNumber,entryNumber);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.termQuery("_id", id));
		searchSourceBuilder.version(true);
		SearchRequest request = new SearchRequest();
		request.indices(indexName);
		request.source(searchSourceBuilder);

		SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
		SearchHit[] searchHits = response.getHits().getHits();

		if(searchHits.length == 0){
			version = 0;
		}else {
			version = searchHits[0].getVersion();
		}
		return version;

	}

	public static class EsException extends Exception {

		public EsException(String s){
			super(s);
		}
	}

	public static class EsConcurrencyException extends Exception {

		public EsConcurrencyException(String s){
			super(s);
		}
	}
}


