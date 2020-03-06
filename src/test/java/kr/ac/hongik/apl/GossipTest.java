package kr.ac.hongik.apl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class GossipTest {
	private static Map<Integer, PublicKey> replicaPublicKeyMap = new HashMap<>();
	private static Map<Integer, PrivateKey> replicaPrivateKeyMap = new HashMap<>();
	private static Properties properties;

	@BeforeAll
	public static void setDependencies() throws Exception {
		InputStream in = Util.getInputStreamOfGivenResource("replica.properties");
		properties = new Properties();
		properties.load(in);

		int replicas = Integer.parseInt(properties.getProperty("replica"));

		replicaPublicKeyMap = new HashMap<>();
		for (int i = 0; i < replicas; i++) {
			KeyPair keyPair = Util.generateKeyPair();

			replicaPublicKeyMap.put(i, keyPair.getPublic());
			replicaPrivateKeyMap.put(i, keyPair.getPrivate());
		}
	}

	@Test
	public void runGossipHandlerTest() throws Exception {
		int clientReplicaNum = 0;
		int handlerReplicaNum = 0;

		Logger logger = new Logger("127.0.0.1", 1234);

		PrivateKey privateKey = replicaPrivateKeyMap.get(handlerReplicaNum);
		PublicKey publicKey = replicaPublicKeyMap.get(handlerReplicaNum);

		Runnable gossipHandlerRunner = () -> {
			new GossipHandler(0, properties, logger, publicKey, privateKey);
		};
		Thread handlerThread = new Thread(gossipHandlerRunner);
		handlerThread.start();

		PrivateKey clientPrivateKey = replicaPrivateKeyMap.get(clientReplicaNum);
		PublicKey clientPublicKey = replicaPublicKeyMap.get(clientReplicaNum);

		GossipClient gossipClient = new GossipClient(properties, replicaPublicKeyMap, clientReplicaNum, clientPublicKey, clientPrivateKey);
		gossipClient.request();

		var map = gossipClient.getReply();
		map.forEach((key, value) -> System.err.printf("%s : %s\n", key, value));
	}

	@Test
	public void runGossipClientTest() throws Exception {
		int clientReplicaNum = 0;

		PrivateKey clientPrivateKey = replicaPrivateKeyMap.get(clientReplicaNum);
		PublicKey clientPublicKey = replicaPublicKeyMap.get(clientReplicaNum);

		GossipClient gossipClient = new GossipClient(properties, replicaPublicKeyMap, clientReplicaNum, clientPublicKey, clientPrivateKey);
		gossipClient.request();

		var map = gossipClient.getReply();
		map.forEach((key, value) -> System.err.printf("%s : %s\n", key, value));
	}
}
