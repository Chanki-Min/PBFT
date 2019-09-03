package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.BlockVerificationOperation;
import kr.ac.hongik.apl.Operations.GetLatestBlockNumberOperation;
import kr.ac.hongik.apl.Operations.Operation;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Monitor extends Client {
	private Runnable verifier = () -> {
		System.err.println("verifier start");
		List<String> indices = Arrays.asList("car_log", "user_log");
		try {
			int maxBlockNumber = getLatestBlockNumber();
			if (maxBlockNumber == -1) {
				System.err.println("Block not exist. Abort verification");
				return;
			}

			for (int blockNumber = 0; blockNumber < maxBlockNumber; blockNumber++) {
				InputStream in = getClass().getResourceAsStream("/replica.properties");
				Properties prop = new Properties();
				prop.load(in);
				Client client = new Client(prop);

				Operation verifyBlockOp = new BlockVerificationOperation(client.getPublicKey(), blockNumber, indices);
				RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), verifyBlockOp);
				client.request(insertRequestMsg);
				List result = (List) client.getReply();

				System.err.printf("%-15s | %-8s | %-8s | %-8s | %-50s | \n", "time", "block#", "entry#", "index", "status");
				System.err.printf("%-15s |", result.get(0));
				result.stream().skip(1).limit(3).forEach(r -> System.err.printf(" %-8s |", r));
				System.err.printf(" %-50s | \n", result.get(4));
			}
		} catch (IOException e) {
			throw new Error(e);
		}
	};
	private int time;
	private TimeUnit timeUnit;

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

		Monitor monitor = new Monitor(properties, time);
		monitor.start();
	}

	private void start() {
		if (Replica.DEBUG) {
			System.err.printf("monitoring start. verify period : %d%s\n", time, timeUnit.toString());
		}
		ScheduledExecutorService verifierSchedule = Executors.newSingleThreadScheduledExecutor();
		verifierSchedule.scheduleWithFixedDelay(verifier, time, time, timeUnit);
	}

	/**
	 * @param
	 * @return latest block_number of BlockChain table in pbft rdbms
	 * @throws IOException
	 */
	private int getLatestBlockNumber() throws IOException {
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		Client client = new Client(prop);

		Operation getLatestBlockNumberOperation = new GetLatestBlockNumberOperation(client.getPublicKey());
		RequestMessage insertRequestMsg = RequestMessage.makeRequestMsg(client.getPrivateKey(), getLatestBlockNumberOperation);
		client.request(insertRequestMsg);
		return (int) client.getReply();
	}
}