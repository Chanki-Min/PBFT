package kr.ac.hongik.apl;

import com.codahale.shamir.Scheme;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static kr.ac.hongik.apl.RequestMessage.makeRequestMsg;

class ValidationTest {
	@Test
	void validationTest() throws IOException {
		/****** Create some blocks to test ******/
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		var replicas = Util.parseProperties(prop);
		String artHash = "art1";

		Client client = new Client(prop);
		BlockCreation blockCreation = new BlockCreation(client.getPublicKey(), artHash, "Alice", "Bob", 100, -1L);
		client.request(makeRequestMsg(client.getPrivateKey(), blockCreation));
		String header = (String) client.getReply();

		int n = replicas.size();
		int f = replicas.size() / 3;
		Map<Integer, byte[]> pieces = Util.split(header, n + 2, n - f + 1);


		//client = new Client(prop);
		CertStorage certStorage = new CertStorage(client.getPublicKey(), pieces);
		client.request(makeRequestMsg(client.getPrivateKey(), certStorage));
		List<Object> roots = (List<Object>) client.getReply();
		String root = (String) roots.get(0);

		/******  Validation process  ******/
		//colllector 실행 -> 키 받기

		int buyerPieceNumber = replicas.size() + 1;
		byte[] buyerCertPiece = pieces.get(buyerPieceNumber);

		//client = new Client(prop);
		Collector collector = new Collector(client.getPublicKey(), root);
		client.request(makeRequestMsg(client.getPrivateKey(), collector));
		List<Object> prePieces = (List<Object>) client.getReply();

		//toMap 이용해서 키 조합
		pieces = Util.toMap(prePieces, buyerPieceNumber, buyerCertPiece);

		Scheme newScheme = new Scheme(new SecureRandom(), n, n - f + 1);
		String newHeader = new String(newScheme.join(pieces));

		//validation
		//client = new Client(prop);
		Validation validation = new Validation(client.getPublicKey(), newHeader, artHash);
		client.request(makeRequestMsg(client.getPrivateKey(), validation));

		Assertions.assertTrue((Boolean) client.getReply());
	}

	@Test
	void faultTest() throws IOException {
		/****** Create some blocks to test ******/
		InputStream in = getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		prop.load(in);
		var replicas = Util.parseProperties(prop);
		String artHash = "art1";

		Client client = new Client(prop);
		BlockCreation blockCreation = new BlockCreation(client.getPublicKey(), artHash, "Alice", "Bob", 100, -1L);
		client.request(makeRequestMsg(client.getPrivateKey(), blockCreation));
		String header = (String) client.getReply();

		int n = replicas.size();
		int f = replicas.size() / 3;
		Map<Integer, byte[]> pieces = Util.split(header, n + 2, n - f + 1);


		//client = new Client(prop);
		CertStorage certStorage = new CertStorage(client.getPublicKey(), pieces);
		client.request(makeRequestMsg(client.getPrivateKey(), certStorage));
		List<Object> roots = (List<Object>) client.getReply();
		String root = (String) roots.get(0);

		/******  Validation process  ******/
		//colllector 실행 -> 키 받기

		int buyerPieceNumber = replicas.size() + 1;
		byte[] buyerCertPiece = pieces.get(buyerPieceNumber);

		//client = new Client(prop);
		Collector collector = new Collector(client.getPublicKey(), root);
		client.request(makeRequestMsg(client.getPrivateKey(), collector));
		List<Object> prePieces = (List<Object>) client.getReply();

		//toMap 이용해서 키 조합
		pieces = Util.toMap(prePieces, buyerPieceNumber, buyerCertPiece);
		pieces.remove(1);

		Scheme newScheme = new Scheme(new SecureRandom(), n, n - f + 1);
		String newHeader = new String(newScheme.join(pieces));

		//validation
		//client = new Client(prop);
		Validation validation = new Validation(client.getPublicKey(), newHeader, artHash);
		client.request(makeRequestMsg(client.getPrivateKey(), validation));

		Assertions.assertFalse((Boolean) client.getReply());
	}
}