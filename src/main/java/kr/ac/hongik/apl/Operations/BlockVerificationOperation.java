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
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.*;

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
	 * cause : All_Pass, etc(error causes)...
	 */
	@Override
	public Object execute(Object obj) {
		Logger logger = (Logger) obj;
		String indexName = null;
		try {
			indexName = getIndexName(blockNumber, indices);
			List<String> result = verifyChain(logger, blockNumber, indexName);
			return result;
		} catch (IOException | EsRestClient.EsSSLException | NoSuchFieldException | SQLException | EsRestClient.EsException e) {
			throw new Error(e);
		}
	}

	private List<String> verifyChain(Logger logger, int blockNumber, String indexName) throws SQLException, IOException, EsRestClient.EsException, NoSuchFieldException, EsRestClient.EsSSLException {
		Genson genson = new GensonBuilder().useClassMetadata(true).useRuntimeType(true).create();
		List<String> verifyLog = new ArrayList<>();
		/*
		Verify Header&Data and get verification logs as OrderedMap
		 */
		LinkedHashMap headerError = verifyHeader(logger, blockNumber);
		List<LinkedHashMap> dataErrors = verifyData(logger, blockNumber, indexName);

		if (headerError != null) {
			verifyLog.add(genson.serialize(headerError));
		}
		if (dataErrors.size() != 0) {
			for (LinkedHashMap error: dataErrors) {
				verifyLog.add(genson.serialize(error));
			}
		}
		if (verifyLog.size() == 0) {
			LinkedHashMap pass = new LinkedHashMap();
			pass.put("type", "result");
			pass.put("timestamp", System.currentTimeMillis());
			pass.put("block_number", blockNumber);
			pass.put("index", indexName);
			pass.put("cause", "All_Passed");
			verifyLog.add(genson.serialize(pass));
		}
		return verifyLog;
	}

	/**
	 * @param logger
	 * @param blockNumber
	 * @throws SQLException
	 * @return null if blockNumbe'th header is verified, LinkedHashMap that contains error info if not
	 */
	private LinkedHashMap verifyHeader(Logger logger, int blockNumber) throws SQLException {
		Triple<Integer, String, String> prevHeader = getHeader(logger, blockNumber - 1);
		Triple<Integer, String, String> currHeader = getHeader(logger, blockNumber);
		Boolean[] checkList = new Boolean[3];

		/*prevHeader == null인 경우는 currHeader = 0 인 경우이므로, curr가 0번째 Header의 preset을 따르는지를 검증한다
		  prevHeader != null인 경우는 각 idx#가 일치하는지, 그리고 curr의 prev가 hash(prevHeader)인지를 검증한다
		 */
		if (prevHeader == null) {
			checkList[0] = currHeader.getLeft() == 0;
			checkList[1] = currHeader.getMiddle().equals("FIRST_ROOT");
			checkList[2] = currHeader.getRight().equals("PREV");
		} else {
			checkList[0] = prevHeader.getLeft() == (blockNumber - 1);
			checkList[1] = currHeader.getLeft() == blockNumber;
			checkList[2] = currHeader.getRight().equals(Util.hash(prevHeader.toString()));
		}

		if (!Arrays.stream(checkList).allMatch(Boolean::booleanValue)) {
			LinkedHashMap map = new LinkedHashMap();
			map.put("exception_type", "header");
			map.put("block_number", blockNumber);
			map.put("is_prev_block_number_match", checkList[0]);
			map.put("is_curr_block_number_match", checkList[1]);
			map.put("is_Hash(preHeader)_match", checkList[2]);
			map.put("cause", "header_broken");
			return map;
		} else {
			return null;
		}
	}

	/**
	 * @param logger
	 * @param blockNum
	 * @param indexName
	 * @return new ArrayList() if blockNumbe'th header is verified, List(LinkedHashMap) that contains error info if not
	 * @throws NoSuchFieldException
	 * @throws SQLException
	 * @throws IOException
	 * @throws EsRestClient.EsException
	 * @throws EsRestClient.EsSSLException
	 */
	private List<LinkedHashMap> verifyData(Logger logger, int blockNum, String indexName) throws NoSuchFieldException, SQLException, IOException, EsRestClient.EsException, EsRestClient.EsSSLException {
		if (blockNum == 0)
			return new ArrayList<>();

		Genson genson = new GensonBuilder().useClassMetadata(true).useRuntimeType(true).create();
		List<Map<String, Object>> restoreMapList = new ArrayList<>();
		List<LinkedHashMap> errors = new ArrayList<>();

		Triple<Integer, String, String> header = getHeader(logger, blockNum);
		SecretKey key = makeSymmetricKey(header.toString());
		var blockDataPair = getBlockDataPair(indexName, blockNumber);

		//If ROOT of PBFT equals to ROOT' that generated by plain data, end verification.
		String rootPrime = new HashTree(blockDataPair.getLeft().stream()
				.map(x -> genson.serialize(x))
				.map(Util::hash)
				.toArray(String[]::new))
				.toString();
		if (header.getMiddle().equals(rootPrime)) {
			return errors;
		} else {
			errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, 0, indexName, "Root_From_PBFT_Not_Same_With_Root'"}));
		}

		//try to decrypt base64 encoding & decrypt encByte[] by generated key & deserialize decByte[] to Map.class
		int entryNum = 0;
		try {
			for (entryNum = 0; entryNum < blockDataPair.getRight().size(); entryNum++) {
				byte[] enc = blockDataPair.getRight().get(entryNum);
				restoreMapList.add(desToObject(new String(decrypt(enc, key)), Map.class));
			}
		} catch (IllegalArgumentException e) {
			entryNum = Integer.parseInt(e.getMessage());
			errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, entryNum, indexName, "Entry_Base64_Decoding_Fail"}));
		} catch (EncryptionException e) {
			errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, entryNum, indexName, "Entry_Decryption_Fail"}));
		} catch (SerializationException s) {
			errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, entryNum, indexName, "Entry_Deserialization_Fail"}));
		}
		//try to compare originalMap == restoreMap
		for (entryNum = 0; entryNum < blockDataPair.getLeft().size(); entryNum++) {
			if (!equals(blockDataPair.getLeft().get(entryNum), restoreMapList.get(entryNum))) {
				errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, entryNum, indexName, "Entry_Plain_NOT_SAME_WITH_RestoredPlain"}));
			}
		}
		return errors;
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

	private Pair<List<Map<String, Object>>, List<byte[]>> getBlockDataPair(String indexName, int blockNumber) throws NoSuchFieldException, EsRestClient.EsSSLException, IOException, EsRestClient.EsException {
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();
		var result = esRestClient.getBlockDataPair(indexName, blockNumber);
		esRestClient.disConnectToEs();
		return  result;
	}

	private boolean equals(Map<String, Object> ori, Map<String, Object> res) {
		if (ori.size() != res.size()) {
			Replica.msgDebugger.debug(String.format("isMapSame::size NOT same. ori: %d, res: %d", ori.size(), res.size()));

			return false;
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

	private LinkedHashMap getDataErrorMap(Object[] entities) throws NoSuchFieldException {
		String[] dataKeys = new String[] {"type", "timestamp", "block_number", "entry_number", "index", "cause"};

		if (dataKeys.length != entities.length)
			throw new NoSuchFieldException();
		LinkedHashMap errorMap = new LinkedHashMap();
		for (int i = 0; i < dataKeys.length; i++) {
			errorMap.put(dataKeys[i], entities[i]);
		}
		return errorMap;
	}

	/**
	 * @param blockNumber blockNumber to find
	 * @param indices     List of "block_chained" indices's names
	 * @throws NoSuchFieldException
	 * @throws IOException
	 * @throws EsRestClient.EsSSLException
	 * @return index name of @param blockNumber
	 */
	private String getIndexName(int blockNumber, List<String> indices) throws NoSuchFieldException, IOException, EsRestClient.EsSSLException {
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();
		String indexName = esRestClient.getIndexNameFromBlockNumber(blockNumber, indices);
		esRestClient.disConnectToEs();
		return indexName;
	}
}