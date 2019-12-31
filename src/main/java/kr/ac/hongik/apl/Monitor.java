package kr.ac.hongik.apl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.BlockVerificationOperation;
import kr.ac.hongik.apl.Operations.GetLatestBlockNumberOperation;
import kr.ac.hongik.apl.Operations.Operation;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Monitor extends Client {
	private int time;
	private TimeUnit timeUnit;
	private List<Long> verificationTimes = (Replica.MEASURE) ? new ArrayList<>() : null;
	private Map<String, Object> esRestClientConfigs;
	ObjectMapper objectMapper = new ObjectMapper()
			.enable(JsonParser.Feature.ALLOW_COMMENTS)
			.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

	private Runnable verifier = () -> {
		Replica.msgDebugger.info(String.format("%s", new String(new char[80]).replace("\0", "=")));
		List<String> indices = List.of("car_log", "user_log");
		int maxBlockNumber = getLatestBlockNumber();
		if (maxBlockNumber <= 0) {
			Replica.msgDebugger.info(String.format("Block not exist. Abort verification"));
			return;
		}
		for (int blockNumber = 0; blockNumber < maxBlockNumber; blockNumber++) {
			Operation verifyBlockOp = new BlockVerificationOperation(super.getPublicKey(),esRestClientConfigs ,blockNumber, indices);
			RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(super.getPrivateKey(), verifyBlockOp);

			if (Replica.MEASURE) {
				if (verificationTimes.size() == 100) {
					double verification_avg = verificationTimes.stream().mapToDouble(x -> x).average().orElse(Double.NaN) / 1000.0;
					Replica.measureDebugger.info(String.format("MEASURE::car_log average of 100 insertion : %f", verification_avg));
				}
				verificationTimes.add(Instant.now().toEpochMilli());
			}
			super.request(insertRequestMsg);
			List<String> result = (List<String>) super.getReply();
			if (Replica.MEASURE) {
				long start = verificationTimes.get(verificationTimes.size() - 1);
				verificationTimes.remove(verificationTimes.size() - 1);
				verificationTimes.add(Instant.now().toEpochMilli() - start);
				Replica.measureDebugger.info(String.format("MEASURE::car_log insertion end with Time : %f", ((double) verificationTimes.get(verificationTimes.size() - 1)) / 1000.0));
			}
			printResult(result);
			Replica.msgDebugger.info(String.format("%s", new String(new char[80]).replace("\0", "=")));
		}
	};

	public Monitor(Properties prop) {
		super(prop);
		this.time = 1;
		this.timeUnit = TimeUnit.HOURS;
	}

	public Monitor(Properties prop, int time) {
		super(prop);
		this.time = time;
		this.timeUnit = TimeUnit.SECONDS;
	}

	/**
	 * @param args [VerifyCation Period as Second]
	 */
	public static void main(String[] args) throws IOException {
		int time = Integer.parseInt(args[0]);
		Properties properties = new Properties();
		InputStream is = Replica.class.getResourceAsStream("/replica.properties");
		properties.load(new java.io.BufferedInputStream(is));

		Monitor monitor;
		if (args.length == 0) {
			monitor = new Monitor(properties);
		} else {
			monitor = new Monitor(properties, time);
		}
		monitor.start();
	}

	private void start() throws IOException {
		Scanner scanner = new Scanner(System.in);

		InputStream in = Monitor.class.getResourceAsStream("/ES_Connection/esRestClient_connection_info.json");
		esRestClientConfigs = objectMapper.readValue(in, Map.class);

		Replica.msgDebugger.info("insert Elasticsearch username : ");
		esRestClientConfigs.put("userName", scanner.next());
		Replica.msgDebugger.info("insert Elasticsearch password : ");
		esRestClientConfigs.put("passWord", scanner.next());
		Replica.msgDebugger.info("insert Elasticsearch certification password : ");
		esRestClientConfigs.put("certPassWord", scanner.next());

		try {
			EsRestClient esRestClient = new EsRestClient(esRestClientConfigs);
			esRestClient.connectToEs();
			esRestClient.getClusterInfo();
			esRestClient.disConnectToEs();
		} catch (Exception e) {
			Replica.msgDebugger.error(e.getMessage());
			System.exit(1);
		}

		Replica.msgDebugger.info(String.format("monitoring start. verify period : %d%s", time, timeUnit.toString()));
		ScheduledExecutorService verifierSchedule = Executors.newSingleThreadScheduledExecutor();
		verifierSchedule.scheduleWithFixedDelay(verifier, time, time, timeUnit);
	}

	/**
	 * @param
	 * @return latest block_number of BlockChain table in pbft rdbms
	 */
	private int getLatestBlockNumber() {
		Operation getLatestBlockNumberOperation = new GetLatestBlockNumberOperation(super.getPublicKey());
		RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(super.getPrivateKey(), getLatestBlockNumberOperation);
		super.request(insertRequestMsg);
		return (int) super.getReply();
	}

	private void printResult(List<String> result) {
		try {
			for (String log: result) {
				LinkedHashMap map = objectMapper.readValue(log, LinkedHashMap.class);
				map.keySet().stream().forEach(k -> Replica.msgDebugger.info(String.format("%-10s : %s", k, map.get(k))));
			}
		} catch (JsonProcessingException e) {
			Replica.msgDebugger.error(e.getMessage());
			throw new Error(e);
		}
	}
}