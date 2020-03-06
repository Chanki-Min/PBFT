package kr.ac.hongik.apl.GarbageCollection;

import kr.ac.hongik.apl.Blockchain.BlockHeader;
import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.Dev.CountMsgsOperation;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.security.PublicKey;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

class SampleOperation extends Operation {

	SampleOperation(PublicKey clientInfo, long timestamp) {
		super(clientInfo);
	}

	@Override
	public Object execute(Object obj) {
		return null;
	}
}

class LoggerTest {
	/**
	 * invokeMethod
	 * private method 테스트를 위한 util
	 */
	@SuppressWarnings("unchecked")
	public static <T> T invokeMethod(Object object, String methodName, Class<T> returnType, Object... args) {
		Class<?>[] parameters = new Class<?>[args.length];
		for (int i = 0; i < args.length; i++) {
			parameters[i] = args[i].getClass();
		}
		try {
			Method method = object.getClass().getDeclaredMethod(methodName, parameters);
			method.setAccessible(true);
			return (T) method.invoke(object, args);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void createDB() {
		Logger logger = new Logger("127.0.0.1", 1234);
		logger.createBlockChainDb("test_blk_1");
	}

	@Test
	void createAndUpdateLatestHeaders() {
		String chainPrefix = "test_blk_";
		int chainNum = 5;
		String root = "test_root";
		Logger logger = new Logger("127.0.0.1", 1234);

		for (int i = 0; i < chainNum; i++) {
			String chainName = chainPrefix + i;

			logger.createBlockChainDb(chainName);
			String query = "INSERT INTO " + Logger.BLOCK_CHAIN + " VALUES ( ?, ?, ?, ? )";
			try (var psmt = logger.getPreparedStatement(chainName, query)) {
				BlockHeader previousBlock = logger.getLatestBlockHeader(chainName);
				Replica.msgDebugger.debug(String.format("previousBlock : %d", previousBlock.getBlockNumber()));
				String prevHash = Util.hash(previousBlock.toString());
				psmt.setInt(1, previousBlock.getBlockNumber() + 1);
				psmt.setString(2, root);
				psmt.setString(3, prevHash);
				psmt.setBoolean(4, false);
				psmt.execute();
			} catch (SQLException e) {
				Replica.msgDebugger.error(e);
				throw new RuntimeException(e);
			}
		}
		invokeMethod(logger, "updateLatestHeaders", void.class);
	}

	@Test
	public void ManyManyClientGC() {
		for (int i = 0; i < 15; i++)
			ManyClientGC();
	}

	@Test
	public void OneClientGC() {
		try {
			InputStream in = Util.getInputStreamOfGivenResource("replica.properties");

			Properties prop = new Properties();
			prop.load(in);

			Client client = new Client(prop);
			Operation op = new CountMsgsOperation(client.getPublicKey());
			RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);

			client.request(requestMessage);
			int[] firstRet = (int[]) client.getReply();

			System.err.print("Table Counts before Test : ");
			Arrays.stream(firstRet).forEach(x -> System.err.print(x + " "));
			System.err.println();
			System.err.println("Client: Request");
			Integer repeatTime = 100;
			for (int i = 1; i <= repeatTime; i++) {
				op = new CountMsgsOperation(client.getPublicKey());
				requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
				client.request(requestMessage);
				int[] ret = (int[]) client.getReply();
				System.err.printf("#%d: ", i);
				Arrays.stream(ret).forEach(x -> System.err.print(x + " "));
				System.err.println();
				//Assertions.assertEquals((firstRet[3] + i) % Replica.WATERMARK_UNIT, ret[3] % Replica.WATERMARK_UNIT);

				Thread.sleep(2000);
			}
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	@Test
	public void ManyClientGC() {
		try {
			InputStream in = Util.getInputStreamOfGivenResource("replica.properties");
			Properties prop = new Properties();
			prop.load(in);

			System.err.println("Countless Client : many");

			Client client = new Client(prop);
			Operation op = new CountMsgsOperation(client.getPublicKey());
			RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);

			client.request(requestMessage);
			int[] firstRet = (int[]) client.getReply();

			System.err.print("Table Counts before Test : ");
			Arrays.stream(firstRet).forEach(x -> System.err.print(x + " "));
			System.err.println();
			var beg = Instant.now().toEpochMilli();
			int maxClientNum = 5;
			int manyClientRequestNum = 4;
			List<Thread> clientThreadList = new ArrayList<>(maxClientNum);
			for (int i = 0; i < maxClientNum; i++) {
				Thread thread = new Thread(new CountlessClientGCThread(prop, i, manyClientRequestNum));
				clientThreadList.add(thread);
			}
			for (var i: clientThreadList) {
				i.start();
			}
			for (var i: clientThreadList) {
				i.join();
			}
			var end = Instant.now().toEpochMilli();
			//sleep(30000);
			op = new CountMsgsOperation(client.getPublicKey());
			requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
			client.request(requestMessage);
			int[] afterRet = (int[]) client.getReply();
			System.err.printf("After Many Requests of Countless Client : " + (end - beg) + "millisec , ");
			Arrays.stream(afterRet).forEach(x -> System.err.print(x + " "));
			System.err.println();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
