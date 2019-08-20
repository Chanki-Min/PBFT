package kr.ac.hongik.apl.ES;


import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.ElasticsearchCorruptionException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.*;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class EsRestClient {
	private String masterJsonPath = "/ES_MappingAndSetting/master.json";
	private String masterJsonKey = "masterHostInfo";
	private List<String> hostNames = new ArrayList<>();
	private List<Integer> ports = new ArrayList<>();
	private List<String> hostSchemes = new ArrayList<>();
	private RestHighLevelClient restHighLevelClient = null;
	private final boolean DEBUG = false;

	public EsRestClient(){
	}

	public void connectToEs() throws NoSuchFieldException, EsSSLException{
		getMasterNodeInfo();
		try {
			final CredentialsProvider credentialsProvider =
					new BasicCredentialsProvider();
			//TODO: superuser가 아닌 계정을 사용하게 변경, passpword 암호화
			credentialsProvider.setCredentials(AuthScope.ANY,
					new UsernamePasswordCredentials("elastic", "wowsan2015@!@#$"));

			String certPath = "/Certificates/esRestClient-cert.p12";
			KeyStore trustStore = KeyStore.getInstance("jks");
			InputStream is = EsRestClient.class.getResourceAsStream(certPath);
			trustStore.load(is, new String("wowsan2015@!@#$").toCharArray());

			SSLContextBuilder sslBuilder = SSLContexts.custom()
					.loadTrustMaterial(trustStore, null);
			final SSLContext sslContext = sslBuilder.build();
			List<HttpHost> httpHosts = new ArrayList<>();
			for (int i = 0; i < hostNames.size(); i++) {
				httpHosts.add(new HttpHost(hostNames.get(i), ports.get(i), hostSchemes.get(i)));
			}
			HttpHost[] httpHostsArr = httpHosts.toArray(new HttpHost[0]);
			RestClientBuilder builder = RestClient.builder(httpHostsArr)
					.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
						@Override
						public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder){
							return httpAsyncClientBuilder.setSSLContext(sslContext)
									.setDefaultCredentialsProvider(credentialsProvider);
						}
					});

			restHighLevelClient = new RestHighLevelClient(builder);
		} catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
			e.printStackTrace();
			throw new EsSSLException(e.getMessage());
		}
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
			throw new ElasticsearchCorruptionException(indexName+ " is Already exists");
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
	 * This store PlainData + encData
	 * @param indexName
	 * @param blockNumber
	 * @param plainDataList original userData
	 * @param encData
	 * @param versionNumber number that stating with "1" and MUST increases whenever document updates
	 * @return	ElasticSearch's BulkResponse instance of  data-insertion
	 * @throws IOException
	 * @throws EsException throws when (index not exists, some of insertion failed)
	 * @throws EsConcurrencyException
	 * throws when headDocument of (indexName,BlockNumber) already exists.
	 * that means other replica already bulkInserting to certain version, so cancel method
	 */
	public BulkResponse bulkInsertDocument(String indexName, int blockNumber, List<Map<String, Object>> plainDataList,List<byte[]> encData, long versionNumber) throws IOException, EsException, EsConcurrencyException{
		if(versionNumber <= getDocumentVersion(indexName,blockNumber,-1))
			throw new EsConcurrencyException("current version is higher than this");

		BulkResponse bulkResponse = null;
		final int BULKBUFFER;
		if(DEBUG)
			BULKBUFFER = 500;
		else
			BULKBUFFER = encData.size() + 1;

		if(!this.isIndexExists(indexName))
			throw new EsException("index :"+indexName+" does not exists");

		//Check plainDataList's mapping equals to Index's mapping
		Set indexKeySet = getFieldKeySet(indexName); indexKeySet.remove("block_number"); indexKeySet.remove("entry_number"); indexKeySet.remove("encrypt_data");
		if(!plainDataList.stream().allMatch(x -> x.keySet().equals(indexKeySet)))
			throw new EsException("index :"+indexName+" field mapping does NOT equal to given plainDataList ketSet");


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

		//make query
		Base64.Encoder encoder = Base64.getEncoder();
		BulkRequest request = new BulkRequest();
		request.setRefreshPolicy("wait_for");	//do not refresh (searchable) index until bulk request finish
		for(int entryNumber = 0; entryNumber<encData.size(); entryNumber++) {

			String id = idGenerator(indexName,blockNumber,entryNumber);
			String base64EncodedData = encoder.encodeToString(encData.get(entryNumber));
			XContentBuilder builder = new XContentFactory().jsonBuilder();
			builder.startObject();
			{
				builder.field("block_number", blockNumber);
				builder.field("entry_number", entryNumber);
				builder.field("encrypt_data", base64EncodedData);
			}
			for(String plainKey : plainDataList.get(entryNumber).keySet()){
				builder.field(plainKey, plainDataList.get(entryNumber).get(plainKey));
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
			System.out.println("BULK_Created, status: "+bulkResponse.status());
			System.out.println(bulkResponse.buildFailureMessage());
			System.out.println("Request sent | Cause: This is Last buffer |");
		}
		return bulkResponse;
	}

	/**
	 * This store PlainData + encData
	 * @param indexName
	 * @param blockNumber
	 * @param plainDataList original userData
	 * @param encData
	 * @param versionNumber number that stating with "1" and MUST increases whenever document updates
	 * @param maxAction limit of request number of One bulk execution
	 * @param maxSize	limit of request all size of One bulk execution
	 * @param maxSizeUnit	unit of maxSize(Kb, Mb, etc...)
	 * @param threadSize	limit of threads
	 * @return	ElasticSearch's BulkResponse instance of  data-insertion
	 * @throws IOException
	 * @throws EsException throws when (index not exists, some of insertion failed)
	 * @throws EsConcurrencyException
	 * throws when headDocument of (indexName,BlockNumber) already exists.
	 * that means other replica already bulkInserting to certain version, so cancel method
	 */
	public void bulkInsertDocumentByProcessor(
			String indexName, int blockNumber, List<Map<String, Object>> plainDataList, List<byte[]> encData, long versionNumber, int maxAction, int maxSize, ByteSizeUnit maxSizeUnit, int threadSize)
			throws IOException, EsException, EsConcurrencyException, InterruptedException{
		if(versionNumber <= getDocumentVersion(indexName,blockNumber,-1))
			throw new EsConcurrencyException("current version is higher than this");

		if(!this.isIndexExists(indexName))
			throw new EsException("index :"+indexName+" does not exists");

		//Check plainDataList's mapping equals to Index's mapping
		Set indexKeySet = getFieldKeySet(indexName); indexKeySet.remove("block_number"); indexKeySet.remove("entry_number"); indexKeySet.remove("encrypt_data");
		if(!plainDataList.stream().allMatch(x -> x.keySet().equals(indexKeySet)))
			throw new EsException("index :"+indexName+" field mapping does NOT equal to given plainDataList ketSet");


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

		BulkProcessor.Listener listener = new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
				//System.err.println("bulk insertion START, LEN :"+request.numberOfActions()+" SIZE :"+request.estimatedSizeInBytes());
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request,
								  BulkResponse response) {
				//System.err.println("bulk insertion OK, LEN :"+request.numberOfActions()+" SIZE :"+request.estimatedSizeInBytes()+" exeID :"+executionId);
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request,
								  Throwable failure) {
				System.err.println("bulk insertion FAIL, cause :"+failure);
			}
		};
		BulkProcessor.Builder processorBuilder = BulkProcessor.builder(
				(req, bulkListener) ->
						restHighLevelClient.bulkAsync(req, RequestOptions.DEFAULT, bulkListener),
				listener);
		processorBuilder.setBulkActions(maxAction);
		processorBuilder.setBulkSize(new ByteSizeValue(maxSize, maxSizeUnit));
		processorBuilder.setConcurrentRequests(threadSize);
		processorBuilder.setBackoffPolicy(BackoffPolicy
				.constantBackoff(TimeValue.timeValueSeconds(1L), 3));
		BulkProcessor bulkProcessor = processorBuilder.build();


		//make query
		Base64.Encoder encoder = Base64.getEncoder();
		for(int entryNumber = 0; entryNumber<encData.size(); entryNumber++) {

			String id = idGenerator(indexName,blockNumber,entryNumber);
			String base64EncodedData = encoder.encodeToString(encData.get(entryNumber));
			XContentBuilder builder = new XContentFactory().jsonBuilder();
			builder.startObject();
			{
				builder.field("block_number", blockNumber);
				builder.field("entry_number", entryNumber);
				builder.field("encrypt_data", base64EncodedData);
			}
			for(String plainKey : plainDataList.get(entryNumber).keySet()){
				builder.field(plainKey, plainDataList.get(entryNumber).get(plainKey));
			}
			builder.endObject();
			bulkProcessor.add(new IndexRequest(indexName).id(id).source(builder).version(versionNumber).versionType(VersionType.EXTERNAL));
		}
		bulkProcessor.awaitClose(10L, TimeUnit.SECONDS);
		//return null;
	}

	/**
	 * @param indexName
	 * @param blockNumber
	 * @return Pair (plain_data_list,encrypted_data) in #blockNumber th block EXCEPT headDocument
	 * @throws IOException
	 * @throws EsException
	 */
	public Pair<List<Map<String,Object>>, List<byte[]>> getBlockDataPair(String indexName, int blockNumber) throws IOException, EsException, NoSuchFieldException{

		if(!isIndexExists(indexName)){
			throw new EsException(indexName+ " does not exists");
		}
		if(!isBlockExists(indexName,blockNumber)){
			throw new EsException(blockNumber+ " does not exists in "+indexName);
		}
		Base64.Decoder decoder = Base64.getDecoder();
		SearchRequest request = new SearchRequest(indexName);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

		boolQueryBuilder.must(QueryBuilders.matchQuery("block_number", blockNumber));
		boolQueryBuilder.mustNot(QueryBuilders.matchQuery("_id", idGenerator(indexName,blockNumber, -1)));

		searchSourceBuilder.query(boolQueryBuilder);
		searchSourceBuilder.sort("entry_number", SortOrder.ASC);	//set sort option to "entry_number" ascending

		searchSourceBuilder.from(0);
		searchSourceBuilder.size(getBlockEntrySize(indexName, blockNumber) -1);		//set search range to [0, index Size], without headDoc

		request.source(searchSourceBuilder);
		SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
		SearchHit[] searchHits = response.getHits().getHits();

		List<Map<String,Object>> plain_data_list = new ArrayList<>();
		List<byte[]> encrypt_data_list = new ArrayList<>();
		Set fileKeySet = getFieldKeySet(indexName);

		for (int i = 0; i < searchHits.length; i++) {
			SearchHit searchHit = searchHits[i];
			var sourceMap = searchHit.getSourceAsMap();
			//가져온 sourceMap 의 KeySet != index KeySet 이면 Data 변조로 확인하고 삭제
			if(!sourceMap.keySet().equals(fileKeySet)) {
				throw new NoSuchFieldException(String.valueOf(searchHit.getId().split("_")[3]));
			}
			//Base64 decoding 실패 시 Illegal Exception 에 문제된 entryNumber 를 담아 Throw
			try {
				encrypt_data_list.add(decoder.decode((String) sourceMap.get("encrypt_data")));
			} catch (IllegalArgumentException e) {
				throw new IllegalArgumentException(String.valueOf(searchHit.getId().split("_")[3]));
			}
			sourceMap.keySet().removeAll(Set.of("block_number", "entry_number", "encrypt_data"));
			plain_data_list.add(sourceMap);
		}
		return Pair.of(plain_data_list, encrypt_data_list);
	}

	public Pair<Map<String, Object>, byte[]> getBlockEntryDataPair(String indexName, int blockNumber, int entryNumber) throws IOException, EsException{

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

		byte[] encrypt_data;
		Map<String, Object> plain_data;
		encrypt_data = decoder.decode((String) searchHits[0].getSourceAsMap().get("encrypt_data"));
		plain_data = searchHits[0].getSourceAsMap();
		plain_data.keySet().removeAll(Set.of("block_number", "entry_number", "encrypt_data"));

		return Pair.of(plain_data, encrypt_data);
	}

	/**
	 * read "resources/master.json" and parse to httpHost format
	 * @throws NoSuchFieldException
	 */
	private void getMasterNodeInfo() throws NoSuchFieldException{
		try {
			EsJsonParser esJsonParser = new EsJsonParser();
			esJsonParser.setFilePath(masterJsonPath);
			List<Map> masterMap = esJsonParser.listedJsonFileToList(masterJsonKey);

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

	public Set getFieldKeySet(String indexName) throws IOException{
		GetMappingsRequest request = new GetMappingsRequest();
		request.indices(indexName);
		GetMappingsResponse response = restHighLevelClient.indices().getMapping(request, RequestOptions.DEFAULT);
		Map<String, MappingMetaData> mapping = response.mappings();
		MappingMetaData data = mapping.get(indexName);
		return  ((Map) data.getSourceAsMap().get("properties")).keySet();
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

	public int getRightNextBlockNumber(String indexName) throws IOException{
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
		builder.query(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("block_number", blockNumber)));
		request.source(builder);
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

	public static class EsSSLException extends  Exception {
		public EsSSLException(String s) { super(s);}
	}
}


