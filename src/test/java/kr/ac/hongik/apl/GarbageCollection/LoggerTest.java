package kr.ac.hongik.apl.GarbageCollection;

import kr.ac.hongik.apl.Client;
import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Messages.RequestMessage;
import kr.ac.hongik.apl.Operations.Dev.CountMsgsOperation;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Replica;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.security.PublicKey;
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

	@Test
	void createTables() {
		Logger logger = new Logger();

	}

	@Test
	public void ManyManyClientGC() {
		for (int i = 0; i < 15; i++)
			ManyClientGC();
	}

	@Test
	public void OneClientGC() {
		try {
			InputStream in = getClass().getResourceAsStream("/replica.properties");

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
			}
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	@Test
	public void ManyClientGC() {
		try {
			InputStream in = getClass().getResourceAsStream("/replica.properties");
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
			Assertions.assertEquals((firstRet[3] + (maxClientNum * manyClientRequestNum) + 1) % Replica.WATERMARK_UNIT, afterRet[3] % Replica.WATERMARK_UNIT);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
