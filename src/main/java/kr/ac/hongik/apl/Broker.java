package kr.ac.hongik.apl;

import com.owlike.genson.Genson;
import com.owlike.genson.GensonBuilder;
import kr.ac.hongik.apl.Blockchain.HashTree;
import kr.ac.hongik.apl.ES.EsJsonParser;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Generator.Generator;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.GetHeaderOperation;
import kr.ac.hongik.apl.Operations.InsertHeaderOperation;
import kr.ac.hongik.apl.Operations.Operation;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;
import static kr.ac.hongik.apl.Util.makeSymmetricKey;

public class Broker {
	private int waitTime;
	private int maxQueue;
	final String[] indicesPath = new String[] {
			"/ES_MappingAndSetting/Mapping_car_log.json",
			"/ES_MappingAndSetting/Mapping_user_log.json"
	};
	final String settingPath = "/ES_MappingAndSetting/Setting.json";
	private List<String> initDataPaths;
	private boolean isDataLoop;
	private EsRestClient esRestClient;
	private List<Long> car_log_insertionTimes = (Replica.MEASURE) ? new ArrayList<>() : null;
	private List<Long> user_log_insertionTimes = (Replica.MEASURE) ? new ArrayList<>() : null;

	public Broker(List<String> initDataPaths, int waitTime, int maxQueue, boolean isDataLoop) {
		try {
			Map<String, Object> esRestClientConfigs = new HashMap<>();
			esRestClientConfigs.put("userName", "apl");
			esRestClientConfigs.put("passWord", "wowsan2015@!@#$");
			esRestClientConfigs.put("certPath", "/ES_Connection/esRestClient-cert.p12");
			esRestClientConfigs.put("certPassWord", "wowsan2015@!@#$");

			Map<String, Object> masterMap = new HashMap<>();
			masterMap.put( "name", "es01-master01");
			masterMap.put( "hostName", "223.194.70.111");
			masterMap.put( "port", "51192");
			masterMap.put( "hostScheme", "https");

			esRestClientConfigs.put("masterHostInfo", List.of(masterMap));


			this.initDataPaths = initDataPaths;
			this.waitTime = waitTime;
			this.maxQueue = maxQueue;
			this.isDataLoop = isDataLoop;
			this.esRestClient = new EsRestClient(esRestClientConfigs);
			esRestClient.connectToEs();
		} catch (EsRestClient.EsSSLException | NoSuchFieldException e) {
			throw new Error(e);
		}
	}

	public static void main(String[] args) {
		List<String> initDataPaths = new ArrayList<>();
		initDataPaths.add("/GEN_initData/Init_data00.json");
		initDataPaths.add("/GEN_initData/Init_data01.json");
		initDataPaths.add("/GEN_initData/Init_data02.json");
		initDataPaths.add("/GEN_initData/Init_data03.json");

		Broker broker = new Broker(initDataPaths, Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2].equals("true"));
		broker.start();
	}

	private void start() {
		Queue<Map> car_log_queue = new ArrayDeque<>();
		Queue<Map> user_log_queue = new ArrayDeque<>();
		List<Generator> generators = new ArrayList<>();
		initDataPaths.stream().forEachOrdered(path -> generators.add(new Generator(path, isDataLoop)));
		try {
			for (var mappingPath: indicesPath) {
				String indexName = getIndexNameFromFilePath(mappingPath);
				if (!esRestClient.isIndexExists(indexName)) {
					EsJsonParser esJsonParser = new EsJsonParser();
					esJsonParser.setFilePath(mappingPath);
					XContentBuilder mappingBuilder = esJsonParser.jsonFileToXContentBuilder(false);
					esJsonParser.setFilePath(settingPath);
					XContentBuilder settingBuilder = esJsonParser.jsonFileToXContentBuilder(false);
					esRestClient.createIndex(indexName, mappingBuilder, settingBuilder);
				}
			}

			while (true) {
				for (int i = 0; i < generators.size(); i++) {
					try {
						var pair = generators.get(i).generate();
						car_log_queue.offer(pair.getLeft());
						user_log_queue.offer(pair.getRight());
					} catch (NoSuchFieldException e) {
						System.err.printf("remove generator #%d\n", i);
						generators.remove(i);
					}
				}

				if (car_log_queue.size() >= maxQueue) {
					if (Replica.MEASURE) {
						car_log_insertionTimes.add(Instant.now().toEpochMilli());
					}
					System.err.printf("car_log MQ FULL, Inserting data to ES&PBFT\n");
					System.err.printf("data size : %d\n", car_log_queue.size());

					String indexName = "car_log";
					List dataList = car_log_queue.stream().collect(Collectors.toList());
					car_log_queue.clear();

					//get latest block# from ElasticSearch and plus 1
					int blockNumber = getLatestBlockNumber(Arrays.asList("car_log", "user_log")) + 1;
					HashTree hashTree = storeDataToEs(indexName, blockNumber, dataList);
					storeHeaderToPBFT(blockNumber, hashTree.toString());
					System.err.printf("car_log insertion end\n");

					if (Replica.MEASURE) {
						long start = car_log_insertionTimes.get(car_log_insertionTimes.size() - 1);
						car_log_insertionTimes.remove(car_log_insertionTimes.size() - 1);
						car_log_insertionTimes.add(Instant.now().toEpochMilli() - start);
						System.err.printf("MEASURE::car_log insertion end with Time : %f\n", ((double) car_log_insertionTimes.get(car_log_insertionTimes.size() - 1)) / 1000.0);
					}


					System.err.printf("%s\n", new String(new char[80]).replace("\0", "="));
					sleep(waitTime);
				}
				if (user_log_queue.size() == maxQueue) {
					if (Replica.MEASURE) {
						user_log_insertionTimes.add(Instant.now().toEpochMilli());
					}
					System.err.printf("user_log MQ FULL, Inserting data to ES&PBFT\n");
					System.err.printf("data size : %d\n", user_log_queue.size());

					String indexName = "user_log";
					List dataList = user_log_queue.stream().collect(Collectors.toList());
					user_log_queue.clear();

					//get latest block# from ElasticSearch and plus 1
					int blockNumber = getLatestBlockNumber(Arrays.asList("car_log", "user_log")) + 1;
					HashTree hashTree = storeDataToEs(indexName, blockNumber, dataList);
					storeHeaderToPBFT(blockNumber, hashTree.toString());
					System.err.printf("user_log insertion end\n");

					if (Replica.MEASURE) {
						long start = user_log_insertionTimes.get(user_log_insertionTimes.size() - 1);
						user_log_insertionTimes.remove(user_log_insertionTimes.size() - 1);
						user_log_insertionTimes.add(Instant.now().toEpochMilli() - start);
						System.err.printf("MEASURE::user_log insertion end with Time : %f\n", ((double) user_log_insertionTimes.get(user_log_insertionTimes.size() - 1)) / 1000.0);
					}
					System.err.printf("%s\n", new String(new char[80]).replace("\0", "="));
					sleep(waitTime);
				}
				if (Replica.MEASURE) {
					if (car_log_insertionTimes.size() == 100) {
						List<Long> all_logs = new ArrayList();
						all_logs.addAll(car_log_insertionTimes);
						all_logs.addAll(user_log_insertionTimes);
						double car_log_avg = car_log_insertionTimes.stream().mapToDouble(x -> x).average().orElse(Double.NaN) / 1000.0;
						double user_log_avg = user_log_insertionTimes.stream().mapToDouble(x -> x).average().orElse(Double.NaN) / 1000.0;
						double all_log_avg = all_logs.stream().mapToDouble(x -> x).average().orElse(Double.NaN) / 1000.0;
						System.err.printf("MEASURE::car_log average of 100 insertion : %f\n", car_log_avg);
						System.err.printf("MEASURE::user_log average of 100 insertion : %f\n", user_log_avg);
						System.err.printf("MEASURE::all_log average of 100 insertion : %f\n", all_log_avg);
						return;
					}
				}
			}
		} catch (NoSuchFieldException | EsRestClient.EsSSLException | IOException | Util.EncryptionException | InterruptedException | EsRestClient.EsConcurrencyException | EsRestClient.EsException e) {
			throw new Error(e);
		}
	}

	private HashTree storeDataToEs(String indexName, int blockNumber, List<Map<String, Object>> dataList) throws IOException, Util.EncryptionException, EsRestClient.EsConcurrencyException, InterruptedException, EsRestClient.EsException {
		Genson genson = new GensonBuilder().useRuntimeType(true).useClassMetadata(true).create();
		//get previous Header from PBFT
		Triple<Integer, String, String> prevHeader = getHeader(blockNumber - 1);

		//create all data [block#, entry#, cipher, planMap]
		HashTree hashTree = new HashTree(dataList.stream().map(x -> genson.serialize(x)).map(Util::hash).toArray(String[]::new));
		Triple<Integer, String, String> currHeader = new ImmutableTriple<>(blockNumber, hashTree.toString(), Util.hash(prevHeader.toString()));
		SecretKey key = makeSymmetricKey(currHeader.toString());

		System.err.println("currHeader = " + currHeader.toString());

		List<byte[]> cipher = new ArrayList<>();
		for (Map<String, Object> x: dataList) {
			byte[] encrypt = Util.encrypt(Util.serToString((Serializable) x).getBytes(), key);
			cipher.add(encrypt);
		}
		//insert [block#, entry#, cipher, planMap] to ElasticSearch
		esRestClient.bulkInsertDocumentByProcessor(indexName, blockNumber, dataList, cipher,
				1, 1000, 10, ByteSizeUnit.MB, 5);

		return hashTree;
	}

	private void storeHeaderToPBFT(int blockNumber, String root) throws IOException {
		//send [block#, root] to PBFT to PBFT generates Header and store to sqliteDB itself
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		Client client = new Client(prop);

		Operation insertHeaderOp = new InsertHeaderOperation(client.getPublicKey(), blockNumber, root);
		RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), insertHeaderOp);
		client.request(insertRequestMsg);
		int result = (int) client.getReply();
	}

	/**
	 * @param indices list of search target indices
	 * @return latest block_number of indices
	 * @throws NoSuchFieldException
	 * @throws IOException
	 * @throws EsRestClient.EsSSLException
	 */
	private int getLatestBlockNumber(List<String> indices) throws NoSuchFieldException, IOException, EsRestClient.EsSSLException {
		SearchRequest searchMaxRequest = new SearchRequest(indices.toArray(new String[indices.size()]));
		SearchSourceBuilder maxBuilder = new SearchSourceBuilder();

		maxBuilder.query(QueryBuilders.matchAllQuery());
		MaxAggregationBuilder aggregation = AggregationBuilders.max("maxValueAgg").field("block_number");
		maxBuilder.aggregation(aggregation);

		searchMaxRequest.source(maxBuilder);
		SearchResponse response = esRestClient.getClient().search(searchMaxRequest, RequestOptions.DEFAULT);

		if (response.getHits().getTotalHits().value == 0) {
			return 0;
		}
		ParsedMax maxValue = response.getAggregations().get("maxValueAgg");    //get max_aggregation from response
		return (int) maxValue.getValue();
	}

	private Triple<Integer, String, String> getHeader(int blockNumber) throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);

		Client client = new Client(prop);

		Operation getHeaderOp = new GetHeaderOperation(client.getPublicKey(), blockNumber);
		RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), getHeaderOp);
		client.request(insertRequestMsg);
		return (Triple<Integer, String, String>) client.getReply();
	}

	/**
	 * @param fileName String formatted ~Mapping_[target]*.json
	 * @return [target]*
	 */
	private String getIndexNameFromFilePath(String fileName) {
		Pattern pattern = Pattern.compile("(?<=(Mapping_)).*(?=(.json))");
		Matcher matcher = pattern.matcher(fileName);
		if (matcher.find())
			return matcher.group();
		else
			return null;
	}
}
