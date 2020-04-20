package kr.ac.hongik.apl;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Properties;

public class LoadRSAKeyFromFileTest {

	/**
	 * Util에 정적으로 정의된 메소드를 이용하여 파일에서 pri/pub keypair를 불러온다
	 */
	@Test
	public void loadKeyPair() throws Exception {
		InputStream is = Util.getInputStreamOfGivenResource("replica.properties");
		Properties properties = new Properties();
		properties.load(is);

		String privatePath = properties.getProperty("replica0.privateKey.path");
		String publicPath = properties.getProperty("replica0.publicKey.path");

		privatePath = Util.getCurrentProgramDir() + "/" + privatePath;
		publicPath = Util.getCurrentProgramDir() + "/" + publicPath;

		PrivateKey privateKey = Util.loadPKCS8PemPrivateKey(privatePath, "rsa");
		PublicKey publicKey = Util.loadX509PemPublicKey(publicPath, "rsa");
	}
}
