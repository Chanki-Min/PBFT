package kr.ac.hongik.apl.Operations;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import kr.ac.hongik.apl.Blockchain.BlockHeader;
import kr.ac.hongik.apl.Blockchain.HashTree;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;

import java.io.IOException;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.*;

import static kr.ac.hongik.apl.Util.hash;

public class VerifyBlockOperation extends Operation {
	private ObjectMapper objectMapper = null;
	private String chainName;
	private int blockNumber;
	private Map<String, Object> esRestClientConfigs;
	private EsRestClient esRestClient;

	public VerifyBlockOperation(PublicKey publicKey, Map<String, Object> esRestClientConfigs, String chainName, int blockNumber) {
		super(publicKey);
		this.esRestClientConfigs = esRestClientConfigs;
		this.chainName = chainName;
		this.blockNumber = blockNumber;
	}

	/**
	 * @param obj logger
	 * @return List(Object) of [ timestamp, blockNumber, entryNumber, indexName, cause ],
	 * cause : All_Pass, etc(error causes)...
	 */
	@Override
	public Object execute(Object obj) throws OperationExecutionException {
		this.objectMapper = new ObjectMapper()
				.enable(JsonParser.Feature.ALLOW_COMMENTS)
				.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		Logger logger = (Logger) obj;
		String indexName;
		try {
			esRestClient = new EsRestClient(esRestClientConfigs);
			esRestClient.connectToEs();

			indexName = esRestClient.getIndexNameFromChainNameAndBlockNumber(chainName, blockNumber);
			List<String> result = verifyChain(logger, blockNumber, indexName);
			esRestClient.close();
			return result;
		} catch (IOException | EsRestClient.EsSSLException | NoSuchFieldException | EsRestClient.EsException e) {
			throw new OperationExecutionException(e);
		}
	}

	/*
	Verify Header&Data and get verification logs as OrderedMap
	 */
	private List<String> verifyChain(Logger logger, int blockNumber, String indexName) throws IOException, EsRestClient.EsException, NoSuchFieldException {
		List<String> verifyLog = new ArrayList<>();

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
	 * @return null if blockNumbe'th header is verified, LinkedHashMap that contains error info if not
	 */
	private LinkedHashMap verifyHeader(Logger logger, int blockNumber) {
		BlockHeader prevHeader = logger.getBlockHeader(chainName, blockNumber-1);
		BlockHeader currHeader = logger.getBlockHeader(chainName, blockNumber);
		Boolean[] checkList = new Boolean[3];

		/*prevHeader == null인 경우는 currHeader = 0 인 경우이므로, curr가 0번째 Header의 preset을 따르는지를 검증한다
		  prevHeader != null인 경우는 각 idx#가 일치하는지, 그리고 curr의 prev가 hash(prevHeader)인지를 검증한다
		 */
		if (prevHeader == null) {
			checkList[0] = currHeader.getBlockNumber() == 0;
			checkList[1] = currHeader.getRootHash().equals("FIRST_ROOT");
			checkList[2] = currHeader.getPrevHash().equals("PREV");
			checkList[2] = currHeader.getHasHashList().equals(false);
		} else {
			checkList[0] = prevHeader.getBlockNumber() == (blockNumber - 1);
			checkList[1] = currHeader.getBlockNumber() == blockNumber;
			checkList[2] = currHeader.getPrevHash().equals(Util.hash(prevHeader.toString()));
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
	 * @param blockNumer
	 * @param indexName
	 * @return new ArrayList() if blockNumbe'th header is verified, List(LinkedHashMap) that contains error info if not
	 * @throws NoSuchFieldException
	 * @throws IOException
	 * @throws EsRestClient.EsException
	 */
	private List<LinkedHashMap> verifyData(Logger logger, int blockNumer, String indexName) throws NoSuchFieldException, IOException, EsRestClient.EsException {
		if (blockNumer == 0)
			return new ArrayList<>();

		List<LinkedHashMap> errors = new ArrayList<>();

		BlockHeader header = logger.getBlockHeader(chainName, blockNumber);
		List<Map<String, Object>> plainDataList = esRestClient.getBlockData(indexName, chainName, blockNumber);

		//먼저 ES에서 가져온 plainData의 hash를 계산한다.
		List<String> plainHashList = new ArrayList<>();
		for (Map<String, Object> e: plainDataList) {
			String plainEntry = objectMapper.writeValueAsString(e);
			String hash = hash(plainEntry);
			plainHashList.add(hash);
		}

		//ES의 plain으로부터 hash root'을 계산한다
		String rootPrime = new HashTree(plainHashList.toArray(new String[0])).toString();

		//TODO : 오류 발생시 케이스를 에러코드를 통한 전달로 바꾸기, 만약 ES에서 entry_num, block_num 등을 건들 경우의 대책 강구하기
		if (header.getRootHash().equals(rootPrime)) {
			return errors;
		} else {
			errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, -1, indexName, "New root made by plainData does not match with PBFT's root"}));
		}

		if(!isBlockHasHashList(logger, blockNumber))
			return errors;

		List<String> hashes = getHashList(logger, blockNumber);
		if(hashes.size() != plainDataList.size()) {
			errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, -1, indexName, "hashList's and plainData's size does not match"}));
			return errors;
		}
		for(int entryNum=0; entryNum < plainDataList.size(); entryNum++) {
			if(!hashes.get(entryNum).equals(plainHashList.get(entryNum))) {
				errors.add(getDataErrorMap(new Object[] {"data", System.currentTimeMillis(), blockNumber, entryNum, indexName, "Entry's hash does not match with replica's hash"}));
			}
		}
		return errors;
	}

	private boolean isBlockHasHashList(Logger logger, int blockNumber) {
		String query = "SELECT hasHashList FROM " + Logger.BLOCK_CHAIN + " WHERE idx = ?";

		try (var psmt = logger.getPreparedStatement(chainName, query)){
			psmt.setInt(1, blockNumber);
			var rs = psmt.executeQuery();

			return rs.getBoolean(1);
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private List<String> getHashList(Logger logger, int blockNumber) {
		String query = "SELECT hash FROM " + Logger.HASHES + " WHERE idx = ?";
		List<String> hashList;

		try(var psmt = logger.getPreparedStatement(chainName, query)) {
			psmt.setInt(1, blockNumber);
			var rs = psmt.executeQuery();

			return Util.desToObject(rs.getString(1), List.class);
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
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
