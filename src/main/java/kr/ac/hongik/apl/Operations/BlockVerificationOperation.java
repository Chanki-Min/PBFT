package kr.ac.hongik.apl.Operations;

import com.owlike.genson.Genson;
import com.owlike.genson.GensonBuilder;
import kr.ac.hongik.apl.Blockchain.HashTree;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.Serializable;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static kr.ac.hongik.apl.Replica.DEBUG;
import static kr.ac.hongik.apl.Util.*;

public class BlockVerificationOperation extends Operation {
	private final String tableName = "BlockChain";
	private int blockNumber;
	private List<String> indices;

	public BlockVerificationOperation(PublicKey publicKey, int blockNumber, List<String> indices) {
		super(publicKey);
		this.blockNumber = blockNumber;
		this.indices = indices;
	}

	/**
	 * @param obj logger
	 * @return List(Object) of [ timestamp, blockNumber, entryNumber, indexName, cause ],
	 * cause : All_Pass, etc...
	 */
	@Override
	public Object execute(Object obj) {
		Logger logger = (Logger) obj;
		String indexName = null;
		try {
			indexName = getIndexName(blockNumber, indices);
			verifyChain(logger, blockNumber, indexName);

		} catch (IOException | EsRestClient.EsSSLException | NoSuchFieldException | SQLException | EsRestClient.EsException e) {
			throw new Error(e);
		} catch (DataVerificationException e) {
			System.err.println(e);
			List<Object> log = new ArrayList<Object>();
			log.add(e.timestamp);
			log.add(e.blockNumber);
			log.add(e.entryNumber);
			log.add(e.indexName);
			log.add(e.cause);
			return log;
		} catch (HeaderVerifyException e) {
			System.err.println(e);
			List<Object> log = new ArrayList<Object>();
			log.add(System.currentTimeMillis());
			log.add(blockNumber);
			log.add(0);
			log.add(indexName);
			log.add(e.getMessage());
			return log;
		}
		List<Object> log = new ArrayList<Object>();
		log.add(System.currentTimeMillis());
		log.add(blockNumber);
		log.add(0);
		log.add(indexName);
		log.add("All_Passed");
		return log;
	}

	private String getIndexName(int blockNumber, List<String> indices) throws NoSuchFieldException, IOException, EsRestClient.EsSSLException {
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();
		String indexName = esRestClient.getIndexNameFromBlockNumber(blockNumber, indices);
		esRestClient.disConnectToEs();
		return indexName;
	}

	private void verifyChain(Logger logger, int blockNumber, String indexName) throws SQLException, HeaderVerifyException, DataVerificationException, IOException, EsRestClient.EsException, NoSuchFieldException, EsRestClient.EsSSLException {
		verifyHeader(logger, blockNumber);
		if (Replica.DEBUG) System.err.println("Blk#" + blockNumber + " HEADER PASS");
		verifyData(logger, blockNumber, indexName);
		if (Replica.DEBUG) System.err.println("Blk#" + blockNumber + " ELASTIC PASS");

	}

	private void verifyHeader(Logger logger, int i) throws SQLException, HeaderVerifyException {
		Triple<Integer, String, String> prevHeader = getHeader(logger, i - 1);
		Triple<Integer, String, String> currHeader = getHeader(logger, i);
		Boolean[] checkList = new Boolean[3];

		/*prevHeader == null인 경우는 currHeader = 0 인 경우이므로, curr가 0번째 Header의 preset을 따르는지를 검증한다
		  prevHeader != null인 경우는 각 idx#가 일치하는지, 그리고 curr의 prev가 hash(prevHeader)인지를 검증한다
		 */
		if (prevHeader == null) {
			checkList[0] = currHeader.getLeft() == 0;
			checkList[1] = currHeader.getMiddle().equals("FIRST_ROOT");
			checkList[2] = currHeader.getRight().equals("PREV");
		} else {
			checkList[0] = prevHeader.getLeft() == (i - 1);
			checkList[1] = currHeader.getLeft() == i;
			checkList[2] = currHeader.getRight().equals(Util.hash(prevHeader.toString()));
		}

		if (!Arrays.stream(checkList).allMatch(Boolean::booleanValue)) {
			StringBuilder exceptionMsg = new StringBuilder();
			exceptionMsg.append("Error::HeaderVerifyException ")
					.append("CurrHeaderIdx :").append(i).append("\n")
					.append("is_prev_block_number_match :").append(checkList[0]).append("\n")
					.append("is_curr_block_number_match :").append(checkList[1]).append("\n")
					.append("is_Hash(preHeader)_match :").append(checkList[1]).append("\n")
					.append("Header Broken");
			throw new HeaderVerifyException(exceptionMsg.toString());
		}
	}

	private void verifyData(Logger logger, int blockNum, String indexName) throws NoSuchFieldException, SQLException, IOException, EsRestClient.EsException, DataVerificationException, EsRestClient.EsSSLException {
		if (blockNum == 0) return;
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		int entryNum = 0;
		Triple<Integer, String, String> header = getHeader(logger, blockNum);
		if (Replica.DEBUG) {
			System.err.println("VerifyOp::header = " + header.toString());
		}
		SecretKey key = makeSymmetricKey(header.toString());
		List<Map<String, Object>> restoreMapList = new ArrayList<>();
		Pair<List<Map<String, Object>>, List<byte[]>> data = null;

		//try to decrypt base64 encoding & decrypt encByte[] by generated key & deserialize decByte[] to Map.class
		try {
			data = esRestClient.getBlockDataPair(indexName, blockNum);
			for (entryNum = 0; entryNum < data.getRight().size(); entryNum++) {
				byte[] enc = data.getRight().get(entryNum);
				restoreMapList.add(desToObject(new String(decrypt(enc, key)), Map.class));
			}
		} catch (NoSuchFieldException e) {
			if (DEBUG) System.err.println(e.getMessage());
			entryNum = Integer.parseInt(e.getMessage());
			throw new DataVerificationException(System.currentTimeMillis(), blockNumber, entryNum, indexName, "Entry_KeySet_Malformed");
		} catch (IllegalArgumentException e) {
			entryNum = Integer.parseInt(e.getMessage());
			throw new DataVerificationException(System.currentTimeMillis(), blockNumber, entryNum, indexName, "Entry_Base64_Decoding_Fail");
		} catch (EncryptionException e) {
			throw new DataVerificationException(System.currentTimeMillis(), blockNumber, entryNum, indexName, "Entry_Decryption_Fail");
		} catch (SerializationException s) {
			throw new DataVerificationException(System.currentTimeMillis(), blockNumber, entryNum, indexName, "Entry_Deserialization_Fail");
		}

		//try to compare originalMap == restoreMap
		for (entryNum = 0; entryNum < data.getLeft().size(); entryNum++) {
			if (!equals(data.getLeft().get(entryNum), restoreMapList.get(entryNum))) {
				throw new DataVerificationException(System.currentTimeMillis(), blockNumber, entryNum, indexName, "Entry_Plain_NOT_SAME_WITH_RestoredPlain");
			}
		}

		//generate root' from restoreMap and compare root == root'
		String rootPrime = new HashTree(restoreMapList.stream()
				.map(x -> (Serializable) x)
				.map(Util::hash)
				.toArray(String[]::new))
				.toString();

		if (!header.getMiddle().equals(rootPrime)) {
			throw new DataVerificationException(System.currentTimeMillis(), blockNumber, entryNum, indexName, "Root_NOT_SAME_WITH_RootPrime_OR_Missing_Document");
		}
	}

	private Triple<Integer, String, String> getHeader(Logger logger, int headerNum) throws SQLException {
		if (headerNum < 0) {
			return null;
		}
		String query = "SELECT b.idx, b.root, b.prev from " + tableName + " AS b WHERE b.idx = " + (headerNum);
		var psmt = logger.getPreparedStatement(query);
		var rs = psmt.executeQuery();
		if (rs.next()) {
			return new ImmutableTriple<>(rs.getInt("idx"), rs.getString("root"), rs.getString("prev"));
		} else {
			throw new SQLException("Table :" + tableName + " headerNum :" + headerNum + ", Not Found");
		}
	}

	private boolean equals(Map<String, Object> ori, Map<String, Object> res) {
		if (ori.size() != res.size()) {
			if (Replica.DEBUG) {
				System.err.printf("isMapSame::size NOT same. ori: %d, res: %d\n", ori.size(), res.size());
			}
			return false;
		}
		if (Replica.DEBUG) {
			System.err.println("=====================");
			System.err.println("isMapSame::ori map");
			ori.keySet().stream().forEachOrdered(k -> System.err.printf("%s : %s\n", k, ori.get(k)));
			System.err.println("isMapSame::res map");
			res.keySet().stream().forEachOrdered(k -> System.err.printf("%s : %s\n", k, res.get(k)));
			System.err.println("=====================");
		}
		/*
		Es에 저장된 cipher 를 deserialize 하는 과정에서 Wrapped Class간 Equals가 의도한 대로 되지 않는 문제가 있음
		이 문제를 피하기 위해서 ori와 res의 json serialize된 String은 같을 것이므로 이를 비교한다.
		 */
		Genson genson = new GensonBuilder().useClassMetadata(true).useRuntimeType(true).create();
		String oriSer = genson.serialize(ori);
		String resSer = genson.serialize(res);

		return oriSer.equals(resSer);
	}

	public static class HeaderVerifyException extends Exception {
		HeaderVerifyException(String s) {
			super(s);
		}
	}

	public static class DataVerificationException extends Exception {
		public long timestamp;
		public int blockNumber;
		public int entryNumber;
		public String indexName;
		public String cause;

		DataVerificationException(long timestamp, int blockNumber, int entryNumber, String indexName, String cause) {
			super(cause);
			this.timestamp = timestamp;
			this.blockNumber = blockNumber;
			this.entryNumber = entryNumber;
			this.indexName = indexName;
			this.cause = cause;
		}
	}
}