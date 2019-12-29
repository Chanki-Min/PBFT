package kr.ac.hongik.apl.ES;


import kr.ac.hongik.apl.Replica;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
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
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class EsRestClient implements Closeable {
	private Map<String, Object> configs;
	private List<String> hostNames = new ArrayList<>();
	private List<Integer> ports = new ArrayList<>();
	private List<String> hostSchemes = new ArrayList<>();
	private RestHighLevelClient restHighLevelClient = null;

	/**
	 * @param config EsRestClient의 설정값을 담은 맵, 요구되는 필드는 다음과 같다. <br/>
	 * String userName : elasticsearch username을 작성<br/>
	 * String passWord : elasticsearch password를 작성<br/>
	 * String certPath : EsRestClient가 사용할 elasticsearch의 certification.p12파일의 경로<br/>
	 * String certPassWord : .p12 인증서 비밀번호 <br/>
	 * List<\Map> masterHostInfo : Map들의 리스트로 맵의 구조는 다음과 같다<br/>
	 *               String name : elasticsearch node name<br/>
	 *               String hostName : node's hostname<br/>
	 *               String port : node's port<br/>
	 *               String hostScheme : connection Scheme (http, https...)<br/>
	 */
	public EsRestClient(Map<String, Object> config) {
		configs = config;
	}

	public void connectToEs() throws NoSuchFieldException, EsSSLException {
		getMasterNodeInfo(configs);
		try {
			final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();

			credentialsProvider.setCredentials(AuthScope.ANY,
					new UsernamePasswordCredentials((String) configs.get("userName"), (String) configs.get("passWord")));

			KeyStore trustStore = KeyStore.getInstance("jks");
			InputStream is = EsRestClient.class.getResourceAsStream((String) configs.get("certPath"));
			trustStore.load(is, ((String) configs.get("certPassWord")).toCharArray());

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
						public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
							return httpAsyncClientBuilder
									.setSSLContext(sslContext)
									.setDefaultCredentialsProvider(credentialsProvider)
									.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
						}
					});
			restHighLevelClient = new RestHighLevelClient(builder);
		} catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
			throw new EsSSLException(e);
		}
	}

	/**
	 * read masterHostInfo field from configs and return parsed http format data
	 *
	 * @throws NoSuchFieldException
	 */
	private void getMasterNodeInfo(Map<String, Object> configs) throws NoSuchFieldException {
		List<Map> masterMap = (List<Map>) configs.get("masterHostInfo");
		Boolean[] checkList = new Boolean[3];

		for (var masterInfo: masterMap) {
			this.hostNames.add(masterInfo.get("hostName").toString());
			this.ports.add(Integer.parseInt(masterInfo.get("port").toString()));
			this.hostSchemes.add(masterInfo.get("hostScheme").toString());
		}
		checkList[0] = hostNames.stream().distinct().count() == masterMap.size();
		checkList[1] = ports.size() == masterMap.size();
		checkList[2] = hostSchemes.size() == masterMap.size();

		if (Arrays.stream(checkList).allMatch(x -> x)) {
			return;
		} else {
			Replica.msgDebugger.error(String.format("configs property has unexpected format"));
			throw new NoSuchFieldException("configs property has unexpected format");
		}
	}

	public void close() throws IOException {
		restHighLevelClient.close();
	}

	public void deleteIndex(String indexName) throws IOException, EsException {
		boolean isIndexExists = this.isIndexExists(indexName);
		if (!isIndexExists) {
			throw new EsException(indexName + " does not exists");
		}

		DeleteIndexRequest request = new DeleteIndexRequest(indexName);
		AcknowledgedResponse response = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
		if (response.isAcknowledged()) {
			Replica.msgDebugger.info(String.format("%s DELETED from Cluster", indexName));
		} else {
			throw new EsException("Cannot DELETE " + indexName);
		}
	}

	/**
	 * @param indexName
	 * @throws IOException
	 * @throws EsException            throws when creadtion failed
	 * @throws EsConcurrencyException throws when index already created by other replicas
	 */
	public void createIndex(String indexName, XContentBuilder mapping, XContentBuilder setting) throws IOException, EsException, EsConcurrencyException {
		boolean isIndexExists = this.isIndexExists(indexName);
		if (isIndexExists) {
			throw new ElasticsearchCorruptionException(indexName + " is Already exists");
		}
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		request.mapping(mapping);
		request.settings(setting);

		try {
			CreateIndexResponse response = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
			if (response.isAcknowledged()) {
				Replica.msgDebugger.info(String.format("%s Created to Cluster", indexName));
			} else {
				throw new EsException("Cannot CREATE " + indexName);
			}
		} catch (ElasticsearchStatusException e) {
			throw new EsConcurrencyException(e);
		}
	}

	/**
	 * This store PlainData + encData
	 *
	 * @param indexName
	 * @param blockNumber
	 * @param entryList original userData
	 * @param versionNumber number that stating with "1" and MUST increases whenever document updates
	 * @param maxAction     limit of request number of One bulk execution
	 * @param maxSize       limit of request all size of One bulk execution
	 * @param maxSizeUnit   unit of maxSize(Kb, Mb, etc...)
	 * @param threadSize    limit of threads
	 * @return ElasticSearch's BulkResponse instance of  data-insertion
	 * @throws IOException
	 * @throws EsException            throws when (index not exists, some of insertion failed)
	 * @throws EsConcurrencyException throws when headDocument of (indexName,BlockNumber) already exists.
	 *                                that means other replica already bulkInserting to certain version, so cancel method
	 */
	public void newBulkInsertDocumentByProcessor(
			String indexName, int blockNumber, List<Map<String, Object>> entryList, long versionNumber, int maxAction, int maxSize, ByteSizeUnit maxSizeUnit, int threadSize)
			throws IOException, EsException, EsConcurrencyException, InterruptedException {

		if (!this.isIndexExists(indexName))
			throw new EsException("index :" + indexName + " does not exists");

		//Check entryList's mapping equals to Index's mapping
		Set<String> indexKeySet = getFieldKeySet(indexName);
		indexKeySet.remove("block_number");
		indexKeySet.remove("entry_number");
		if (!entryList.stream().allMatch(x -> x.keySet().equals(indexKeySet)))
			throw new EsException("index :" + indexName + " field mapping does NOT equal to given entryList ketSet");

		//insert HeadDocument, when Head already exist for (indexName,blockNumber,versionNumber), throw exception and cancel bulkInsertion
		try {
			restHighLevelClient.index(getHeadDocument(indexName, blockNumber, versionNumber), RequestOptions.DEFAULT);
		} catch (ElasticsearchStatusException e) {
			Replica.msgDebugger.error(e);
			StringBuilder builder = new StringBuilder();
			builder.append(this.getClass().getName()).append("::bulkInsertDocument").append(" ConcurrencyException");
			builder.append(" indexName :").append(indexName).append(" BlockNum :").append(blockNumber);
			builder.append(" Cause :Document inserting already executing by other replica");
			throw new EsConcurrencyException(builder.toString());
		}
		BulkProcessor.Listener listener = new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
				Replica.detailDebugger.trace(String.format( "bulk insertion START, LEN : %s SIZE : %s", request.numberOfActions(), request.estimatedSizeInBytes()));
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request,
								  BulkResponse response) {
				Replica.detailDebugger.trace(String.format( "bulk insertion Success, LEN : %s SIZE : %s exeID : %s", request.numberOfActions(), request.estimatedSizeInBytes(), executionId));
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request,
								  Throwable failure) {
				Replica.msgDebugger.error(String.format("bulk insertion FAIL, cause : %s", failure));
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
		for (int entryNumber = 0; entryNumber < entryList.size(); entryNumber++) {
			String id = generateId(indexName, blockNumber, entryNumber);
			XContentBuilder builder = new XContentFactory().jsonBuilder();
			builder.startObject();
			{
				builder.field("block_number", blockNumber);
				builder.field("entry_number", entryNumber);
			}
			for (String plainKey: entryList.get(entryNumber).keySet()) {
				builder.field(plainKey, entryList.get(entryNumber).get(plainKey));
			}
			builder.endObject();
			bulkProcessor.add(new IndexRequest(indexName).id(id).source(builder).version(versionNumber).versionType(VersionType.EXTERNAL));
		}
		bulkProcessor.awaitClose(10L, TimeUnit.SECONDS);
	}

	/**
	 * This store PlainData + encData
	 *
	 * @param indexName
	 * @param blockNumber
	 * @param plainDataList original userData
	 * @param encData
	 * @param versionNumber number that stating with "1" and MUST increases whenever document updates
	 * @param maxAction     limit of request number of One bulk execution
	 * @param maxSize       limit of request all size of One bulk execution
	 * @param maxSizeUnit   unit of maxSize(Kb, Mb, etc...)
	 * @param threadSize    limit of threads
	 * @return ElasticSearch's BulkResponse instance of  data-insertion
	 * @throws IOException
	 * @throws EsException            throws when (index not exists, some of insertion failed)
	 * @throws EsConcurrencyException throws when headDocument of (indexName,BlockNumber) already exists.
	 *                                that means other replica already bulkInserting to certain version, so cancel method
	 */
	public void bulkInsertDocumentByProcessor(
			String indexName, int blockNumber, List<Map<String, Object>> plainDataList, List<byte[]> encData, long versionNumber, int maxAction, int maxSize, ByteSizeUnit maxSizeUnit, int threadSize)
			throws IOException, EsException, EsConcurrencyException, InterruptedException {

		if (!this.isIndexExists(indexName))
			throw new EsException("index :" + indexName + " does not exists");

		//Check plainDataList's mapping equals to Index's mapping
		Set indexKeySet = getFieldKeySet(indexName);
		indexKeySet.remove("block_number");
		indexKeySet.remove("entry_number");
		indexKeySet.remove("encrypt_data");
		if (!plainDataList.stream().allMatch(x -> x.keySet().equals(indexKeySet)))
			throw new EsException("index :" + indexName + " field mapping does NOT equal to given plainDataList ketSet");

		//insert HeadDocument, when Head already exist for (indexName,blockNumber,versionNumber), throw exception and cancel bulkInsertion
		try {
			restHighLevelClient.index(getHeadDocument(indexName, blockNumber, versionNumber), RequestOptions.DEFAULT);
		} catch (ElasticsearchStatusException e) {
			StringBuilder builder = new StringBuilder();
			builder.append(this.getClass().getName()).append("::bulkInsertDocument").append(" ConcurrencyException");
			builder.append(" indexName :").append(indexName).append(" BlockNum :").append(blockNumber);
			builder.append(" Cause :Document inserting already executing by other replica");
			throw new EsConcurrencyException(builder.toString());
		}
		BulkProcessor.Listener listener = new BulkProcessor.Listener() {
			@Override
			public void beforeBulk(long executionId, BulkRequest request) {
				Replica.msgDebugger.debug(String.format( "bulk insertion START, LEN : %s SIZE : %s", request.numberOfActions(), request.estimatedSizeInBytes()));
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request,
								  BulkResponse response) {
				Replica.msgDebugger.debug(String.format( "bulk insertion Success, LEN : %s SIZE : %s exeID : %s", request.numberOfActions(), request.estimatedSizeInBytes(), executionId));
			}

			@Override
			public void afterBulk(long executionId, BulkRequest request,
								  Throwable failure) {
				Replica.msgDebugger.error(String.format("bulk insertion FAIL, cause : %s", failure));
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
		for (int entryNumber = 0; entryNumber < encData.size(); entryNumber++) {
			String id = generateId(indexName, blockNumber, entryNumber);
			String base64EncodedData = encoder.encodeToString(encData.get(entryNumber));
			XContentBuilder builder = new XContentFactory().jsonBuilder();
			builder.startObject();
			{
				builder.field("block_number", blockNumber);
				builder.field("entry_number", entryNumber);
				builder.field("encrypt_data", base64EncodedData);
			}
			for (String plainKey: plainDataList.get(entryNumber).keySet()) {
				builder.field(plainKey, plainDataList.get(entryNumber).get(plainKey));
			}
			builder.endObject();
			bulkProcessor.add(new IndexRequest(indexName).id(id).source(builder).version(versionNumber).versionType(VersionType.EXTERNAL));
		}
		bulkProcessor.awaitClose(10L, TimeUnit.SECONDS);
	}

	/**
	 * @param indexName
	 * @param blockNumber
	 * @return Pair (plain_data_list, base64 encoded Stirng) in #blockNumber th block EXCEPT headDocument
	 * @throws IOException
	 * @throws EsException
	 */
	public Pair<List<Map<String, Object>>, List<String>> getBlockDataPair(String indexName, int blockNumber) throws IOException, EsException, NoSuchFieldException {
		if (!isIndexExists(indexName)) {
			throw new EsException(indexName + " does not exists");
		}
		if (!isBlockExists(indexName, blockNumber)) {
			throw new EsException(blockNumber + " does not exists in " + indexName);
		}

		SearchRequest request = new SearchRequest(indexName);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

		boolQueryBuilder.must(QueryBuilders.matchQuery("block_number", blockNumber));
		boolQueryBuilder.mustNot(QueryBuilders.matchQuery("_id", generateId(indexName, blockNumber, -1)));

		searchSourceBuilder.query(boolQueryBuilder);
		searchSourceBuilder.sort("entry_number", SortOrder.ASC);    //set sort option to "entry_number" ascending

		searchSourceBuilder.from(0);
		searchSourceBuilder.size(getBlockEntrySize(indexName, blockNumber) - 1);        //set search range to [0, index Size], without headDoc

		request.source(searchSourceBuilder);
		SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
		SearchHit[] searchHits = response.getHits().getHits();

		List<Map<String, Object>> plain_data_list = new ArrayList<>();
		List<String> encrypt_data_list = new ArrayList<>();

		for (SearchHit searchHit: searchHits) {
			var sourceMap = searchHit.getSourceAsMap();
			encrypt_data_list.add((String) sourceMap.get("encrypt_data"));
			sourceMap.keySet().removeAll(Set.of("block_number", "entry_number", "encrypt_data"));
			plain_data_list.add(sourceMap);
		}
		return Pair.of(plain_data_list, encrypt_data_list);
	}

	public List<Map<String, Object>> newGetBlockData(String indexName, int blockNumber) throws EsException, IOException {
		if (!isIndexExists(indexName)) {
			throw new EsException(indexName + " does not exists");
		}
		if (!isBlockExists(indexName, blockNumber)) {
			throw new EsException(blockNumber + " does not exists in " + indexName);
		}

		SearchRequest request = new SearchRequest(indexName);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

		boolQueryBuilder.must(QueryBuilders.matchQuery("block_number", blockNumber));
		boolQueryBuilder.mustNot(QueryBuilders.matchQuery("_id", generateId(indexName, blockNumber, -1)));

		searchSourceBuilder.query(boolQueryBuilder);
		searchSourceBuilder.sort("entry_number", SortOrder.ASC);    //set sort option to "entry_number" ascending

		searchSourceBuilder.from(0);
		searchSourceBuilder.size(getBlockEntrySize(indexName, blockNumber) - 1);        //set search range to [0, index Size], without headDoc

		request.source(searchSourceBuilder);
		SearchResponse response = restHighLevelClient.search(request, RequestOptions.DEFAULT);
		SearchHit[] searchHits = response.getHits().getHits();
		List<Map<String, Object>> plainDataList = new ArrayList<>();

		for (SearchHit searchHit: searchHits) {
			var sourceMap = searchHit.getSourceAsMap();
			sourceMap.keySet().removeAll(Set.of("block_number", "entry_number", "encrypt_data"));
			plainDataList.add(sourceMap);
		}
		return plainDataList;
	}

	public Set<String> getFieldKeySet(String indexName) throws IOException {
		GetMappingsRequest request = new GetMappingsRequest();
		request.indices(indexName);
		GetMappingsResponse response = restHighLevelClient.indices().getMapping(request, RequestOptions.DEFAULT);
		Map<String, MappingMetaData> mapping = response.mappings();
		MappingMetaData data = mapping.get(indexName);
		return ((Map<String, Object>) data.getSourceAsMap().get("properties")).keySet();
	}

	public boolean isIndexExists(String indexName) throws IOException {
		GetIndexRequest getIndexRequest = new GetIndexRequest(indexName);
		return restHighLevelClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
	}

	public boolean isBlockExists(String indexName, int blockNumber) throws IOException {
		CountRequest getCorrupedCount = new CountRequest(indexName);
		SearchSourceBuilder builder = new SearchSourceBuilder();
		builder.query(QueryBuilders.termQuery("block_number", blockNumber));
		getCorrupedCount.source(builder);

		CountResponse response = restHighLevelClient.count(getCorrupedCount, RequestOptions.DEFAULT);
		return response.getCount() != 0;
	}

	/**
	 * @param indices list of search target indices
	 * @return latest block_number of indices
	 * @throws IOException
	 */
	public String getIndexNameFromBlockNumber(int blockNumber, List<String> indices) throws IOException {


		SearchRequest searchRequest = new SearchRequest(indices.toArray(new String[0]));
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

		sourceBuilder.query(QueryBuilders.termQuery("block_number", blockNumber));
		sourceBuilder.size(1);
		sourceBuilder.fetchSource(false);

		searchRequest.source(sourceBuilder);
		SearchResponse response = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

		if (response.getHits().getTotalHits().value == 0) {
			return null;
		}
		return response.getHits().getHits()[0].getIndex();
	}

	private int getBlockEntrySize(String indexName, int blockNumber) throws IOException {
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
	 * @return String that format : indexName_blockNumber_entryNumber
	 */
	private String generateId(String indexName, int blockNumber, int entryNumber) {
		StringBuilder builder = new StringBuilder();
		builder.append(indexName).append("_").append(blockNumber).append("_").append(entryNumber);
		return String.valueOf(builder);
	}

	/**
	 * @param indexName
	 * @param blockNumber
	 * @param versionNumber
	 * @return IndexRequest of indexing headDocument by given params
	 * <pre>{@code
	 * PUT indexName/_doc/indexName_blockNumber_-1?version=versionNumber&version_type="external"
	 * {
	 * 	"block_number", blockNumber,
	 * 	"entry_number", -1
	 * 	"encrypt_data", ""
	 * }
	 * }</pre>
	 * @throws IOException
	 */
	private IndexRequest getHeadDocument(String indexName, int blockNumber, long versionNumber) throws IOException {
		XContentBuilder builder = new XContentFactory().jsonBuilder();
		builder.startObject();
		{
			builder.field("block_number", blockNumber);
			builder.field("entry_number", -1);
		}
		builder.endObject();

		return new IndexRequest(indexName).id(generateId(indexName, blockNumber, -1))
				.source(builder).version(versionNumber).versionType(VersionType.EXTERNAL);
	}

	public RestHighLevelClient getClient() {
		return restHighLevelClient;
	}

	public final ClusterHealthResponse getClusterInfo() throws IOException {
		ClusterHealthRequest clusterHealthRequest = new ClusterHealthRequest();
		ClusterHealthResponse response = restHighLevelClient.cluster().health(clusterHealthRequest, RequestOptions.DEFAULT);
		return response;
	}

	public static class EsException extends Exception {
		public EsException(String s) {
			super(s);
		}
	}

	public static class EsConcurrencyException extends Exception {
		public EsConcurrencyException(String s) {
			super(s);
		}
		public EsConcurrencyException(Exception e) {
			super(e);
		}
	}

	public static class EsSSLException extends Exception {
		public EsSSLException(String s) {
			super(s);
		}
		public EsSSLException(Exception e) {
			super(e);
		}
	}
}