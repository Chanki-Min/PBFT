package kr.ac.hongik.apl.Operations;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import kr.ac.hongik.apl.Blockchain.HashTree;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.*;

import static kr.ac.hongik.apl.Util.*;

public class BlockVerificationOperation extends Operation {
	private final String tableName = "BlockChain";
	private ObjectMapper objectMapper = null;
	private int blockNumber;
	private List<String> indices;
	private Map<String, Object> esRestClientConfigs;
	private EsRestClient esRestClient;

	public BlockVerificationOperation(PublicKey publicKey, Map<String, Object> esRestClientConfigs, int blockNumber, List<String> indices) {
		super(publicKey);
		this.esRestClientConfigs = esRestClientConfigs;
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
		this.objectMapper = new ObjectMapper()
				.enable(JsonParser.Feature.ALLOW_COMMENTS)
				.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		Logger logger = (Logger) obj;
		String indexName;
		try {


			esRestClient = new EsRestClient(esRestClientConfigs);
			esRestClient.connectToEs();
			indexName = esRestClient.getIndexNameFromBlockNumber(blockNumber, indices);
			List<String> result = verifyChain(logger, blockNumber, indexName);
			esRestClient.disConnectToEs();
			return result;
		} catch (IOException | EsRestClient.EsSSLException | NoSuchFieldException | SQLException | EsRestClient.EsException e) {
			throw new Error(e);
		}
	}

	private List<String> verifyChain(Logger logger, int blockNumber, String indexName) throws SQLException, IOException, EsRestClient.EsException, NoSuchFieldException, EsRestClient.EsSSLException {
		List<String> verifyLog = new ArrayList<>();
		/*
		Verify Header&Data and get verification logs as OrderedMap
		 */
		LinkedHashMap headerError = verifyHeader(logger, blockNumber);
		List<LinkedHashMap> dataErrors = verifyData(logger, blockNumber, indexName);

		if (headerError != null) {
			verifyLog.add(objectMapper.writeValueAsString(headerError));
		}
		if (dataErrors.size() != 0) {
			for (LinkedHashMap error: dataErrors) {
				verifyLog.add(objectMapper.writeValueAsString(error));
			}
		}
		if (verifyLog.size() == 0) {
			LinkedHashMap pass = new LinkedHashMap();
			pass.put("type", "result");
			pass.put("timestamp", System.currentTimeMillis());
			pass.put("block_number", blockNumber);
			pass.put("index", indexName);
			pass.put("cause", "All_Passed");
			verifyLog.add(objectMapper.writeValueAsString(pass));
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

		List<Map<String, Object>> restoreMapList = new ArrayList<>();
		List<LinkedHashMap> errors = new ArrayList<>();

		Triple<Integer, String, String> header = getHeader(logger, blockNum);
		SecretKey key = makeSymmetricKey(header.toString());
		var blockDataPair = esRestClient.getBlockDataPair(indexName, blockNumber);

		//먼저 ES에서 가져온 plainData의 hash를 계산한다.
		List<String> plainHashList = new ArrayList<>();
		for (Map<String, Object> x: blockDataPair.getLeft()) {
			String plainEntry = objectMapper.writeValueAsString(x);
			String hash = hash(plainEntry);
			plainHashList.add(hash);
		}

		//ES의 plain으로부터 hash root'을 계산한다
		String rootPrime = new HashTree(plainHashList.toArray(new String[0])).toString();

		//TODO : 오류 발생시 케이스를 에러코드를 통한 전달로 바꾸기
		if (header.getMiddle().equals(rootPrime)) {
			return errors;
		} else {
			errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, -1, indexName, "New root made by plainData does not match with PBFT's root"}));
		}

		//try to decrypt base64 encoding & decrypt encByte[] by generated key & deserialize decByte[] to Map.class
		int entryNum;
		for (entryNum = 0; entryNum < blockDataPair.getRight().size(); entryNum++) {
			try {
				String base64Str = blockDataPair.getRight().get(entryNum);
				byte[] enc = Base64.getDecoder().decode(base64Str);
				String decryptStr = new String(decrypt(enc, key));
				restoreMapList.add(desToObject(decryptStr, Map.class));

				//만약 이 단계까지 Exception 없이 왔다면 d'은 무결하므로 d와 비교한다.
				if(!equals(blockDataPair.getLeft().get(entryNum), restoreMapList.get(entryNum))) {
					errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, entryNum, indexName, "Data has problem, but can be restored by encrypt_data"}));
				}

			} catch (IllegalArgumentException e) {
				errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, entryNum, indexName, "Cipher -> Entry Base64 decoding failure"}));
				restoreMapList.add(null);
			} catch (EncryptionException e) {
				errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, entryNum, indexName, "Cipher -> Entry decryption failure"}));
				restoreMapList.add(null);
			} catch (SerializationException s) {
				errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, entryNum, indexName, "Cipher -> Entry deserialization failure"}));
				restoreMapList.add(null);
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

	private boolean equals(Map<String, Object> ori, Map<String, Object> res) {
		if (ori.size() != res.size()) {
			Replica.msgDebugger.debug(String.format("isMapSame::size NOT same. ori: %d, res: %d", ori.size(), res.size()));

			return false;
		}
		/*
		Es에 저장된 cipher 를 deserialize 하는 과정에서 Wrapped Class간 Equals가 의도한 대로 되지 않는 문제가 있음
		이 문제를 피하기 위해서 ori와 res의 json serialize된 String은 같을 것이므로 이를 비교한다.
		 */
		try {
			String oriSer = objectMapper.writeValueAsString(ori);
			String resSer = objectMapper.writeValueAsString(res);
			return oriSer.equals(resSer);
		} catch (JsonProcessingException e) {
			Replica.msgDebugger.error(e.getMessage());
			throw new Error(e);
		}
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
}