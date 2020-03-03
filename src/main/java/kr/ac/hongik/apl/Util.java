package kr.ac.hongik.apl;

import com.codahale.shamir.Scheme;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang3.SerializationUtils;

import javax.crypto.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.security.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Util {
	static final String ALGORITHM = "SHA1withRSA";

	public static Map<Integer, byte[]> toMap(List<Object> retrieved, int myNumber, byte[] myPiece) {
		Map<Integer, byte[]> ret = new TreeMap<>();

		List<Object[]> input = retrieved.stream().map(x -> (Object[]) x).collect(Collectors.toList());

		ret.put(myNumber, myPiece);

		for (var i: input) {
			ret.put((Integer) i[0], (byte[]) i[1]);
		}

		return ret;
	}

	public static Map<Integer, byte[]> split(String data, int n, int k) {
		Scheme scheme = new Scheme(new SecureRandom(), n, k);
		return scheme.split(data.getBytes());
	}

	public static byte[] concat(byte[] lhs, byte[] rhs) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			bos.write(lhs);
			bos.write(rhs);
			return bos.toByteArray();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @param prop replica.property 파일을 로드한 객체
	 * @return prop에서 파싱된 전체 replica의 주소 리스트
	 */
	public static List<InetSocketAddress> parsePropertiesToAddress(Properties prop) {
		List<InetSocketAddress> replicaAddresses = new ArrayList<>();
		var numOfReplica = Integer.parseInt(prop.getProperty("replica"));

		for (int i = 0; i < numOfReplica; i++) {
			String addressInString = prop.getProperty("replica" + i);
			String[] parsedAddress = addressInString.split(":");

			InetSocketAddress address = new InetSocketAddress(parsedAddress[0], Integer.parseInt(parsedAddress[1]));
			replicaAddresses.add(address);
		}
		return replicaAddresses;
	}

	public static String hash(Serializable obj) {
		return hash(serialize(obj));
	}

	public static String hash(final byte[] bytes) {
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return bytesToHex(digest.digest(bytes));
	}

	public static byte[] serialize(Serializable message) {
		if (message instanceof String)
			return ((String) message).getBytes();
		else
			return SerializationUtils.serialize(message);
	}

	public static String bytesToHex(final byte[] bytes) {
		return IntStream.range(0, bytes.length)
				.collect(StringBuilder::new,
						(sb, i) -> new Formatter(sb).format("%02x", bytes[i] & 0xFF),
						StringBuilder::append).toString();
	}

	/**
	 * Return (private, public) key pair.
	 * To get each keys, use KeyPair.getPublic and KeyPair.getPrivate methods.
	 *
	 * @return KeyPair object
	 * @throws NoSuchAlgorithmException
	 */
	public static KeyPair generateKeyPair() {
		KeyPairGenerator generator = null;
		try {
			generator = KeyPairGenerator.getInstance("RSA");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return generator.generateKeyPair();
	}

	/**
	 * @param key     A private key to identify itself.
	 * @param message Any object to identify the owner.
	 * @return A byte array which contains a digital signature
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @throws NoSuchProviderException
	 */
	public static byte[] sign(PrivateKey key, Serializable message) {
		try {
			Signature signature = Signature.getInstance(ALGORITHM);
			signature.initSign(key);
			signature.update(serialize(message));
			return signature.sign();
		} catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @param key              A public key to check for the owner
	 * @param message          Any object to verify the owner
	 * @param digitalSignature
	 * @return
	 */
	public static boolean verify(PublicKey key, Serializable message, byte[] digitalSignature) {
		try {
			Signature signature = Signature.getInstance(ALGORITHM);
			signature.initVerify(key);
			signature.update(serialize(message));
			return signature.verify(digitalSignature);
		} catch (NoSuchAlgorithmException | SignatureException | InvalidKeyException e) {
			e.printStackTrace();
			return false;
		}
	}

	public static String serToBase64String(Serializable object) {
		return Base64.getEncoder().encodeToString(serialize(object));
	}

	public static <T> T desToObject(String base64Str, Class<T> type) {
		if (type == List.class) {
			ObjectMapper objectMapper = new ObjectMapper()
					.enable(JsonParser.Feature.ALLOW_COMMENTS)
					.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
			try {
				return objectMapper.readValue(base64Str, type);
			} catch (JsonProcessingException e) {
				Replica.msgDebugger.error(e.getMessage());
				throw new Error(e);
			}
			//return new GensonBuilder().useClassMetadata(true).useRuntimeType(true).create().deserialize(base64Str, type);
		} else {
			Serializable obj = deserialize(Base64.getDecoder().decode(base64Str));
			return type.cast(obj);
		}
	}

	public static Serializable deserialize(byte[] bytes) {
		return SerializationUtils.deserialize(bytes);
	}

	public static String serToBase64String(List<?> object) {
		try {
			return new ObjectMapper()
					.enable(JsonParser.Feature.ALLOW_COMMENTS)
					.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
					.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			Replica.msgDebugger.error(e.getMessage());
			throw new Error(e);
		}
		//return new GensonBuilder().useClassMetadata(true).useRuntimeType(true).create().serialize(object);
	}

	public static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
		Set<Object> seen = ConcurrentHashMap.newKeySet();
		return t -> seen.add(keyExtractor.apply(t));
	}

	public static SecretKey makeSymmetricKey(String seed) {
		KeyGenerator gen;
		try {
			gen = KeyGenerator.getInstance("AES");
			SecureRandom rand = null;
			rand = SecureRandom.getInstance("SHA1PRNG");
			rand.setSeed(seed.getBytes());
			gen.init(128, rand);

		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}

		return gen.generateKey();
	}

	public static byte[] encrypt(byte[] plain, SecretKey key) throws EncryptionException {
		try {
			Cipher cipher = Cipher.getInstance("AES");
			cipher.init(Cipher.ENCRYPT_MODE, key);
			return cipher.doFinal(plain);

		} catch (IllegalBlockSizeException | BadPaddingException | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) {
			throw new EncryptionException(e);
		}
	}

	public static byte[] decrypt(byte[] encrypted, SecretKey key) throws EncryptionException {
		try {
			Cipher cipher = Cipher.getInstance("AES");
			cipher.init(Cipher.DECRYPT_MODE, key);
			return cipher.doFinal(encrypted);

		} catch (IllegalBlockSizeException | BadPaddingException | InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) {
			throw new EncryptionException(e);
		}
	}

	public static String getCurrentProgramDir() {
		File jarDir = new File(Util.class.getProtectionDomain().getCodeSource().getLocation().getPath());
		try {
			return jarDir.getParentFile().getCanonicalPath();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * resourceDirectories.properties 에 미리 정의된 필수 설정 파일의 상대 경로부터 리소스 파일의 InputStream을 가져옵니다
	 * 이를 위하여 config 디렉토리는 무조건 .jar 파일과 같이 배포되어야 하며, .jar파일과 같은 디렉토리에 있어야 합니다
	 *
	 * @param resourceName resourceDirectories.properties 에 명시된 리소스 파일의 이름
	 * @return resourceDirectories.properties에 명시된 상대 경로에서 읽어온 리소스 파일의 InputStream
	 */
	public static InputStream getInputStreamOfGivenResource(String resourceName) {
		try {
			InputStream inputStream = Util.class.getResourceAsStream("/resourceDirectories.properties");
			Properties prop = new Properties();
			prop.load(inputStream);

			String relativePath = prop.getProperty(resourceName);
			String absolutePath = Util.getCurrentProgramDir() + "/" + relativePath;

			File resourceFile = new File(absolutePath);
			return new BufferedInputStream(new FileInputStream(resourceFile));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * EncryptionException raises when encryption or decryption is failed.
	 */
	public static class EncryptionException extends Exception {
		public EncryptionException(Throwable reason) {
			super(reason);
		}
	}
}
