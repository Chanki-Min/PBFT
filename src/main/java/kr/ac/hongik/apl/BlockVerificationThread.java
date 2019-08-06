package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Blockchain.HashTree;
import kr.ac.hongik.apl.ES.EsRestClient;
import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.elasticsearch.ElasticsearchException;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static kr.ac.hongik.apl.Util.*;

/**
 *Block Verification Thread, this will run as thread when start() calls <br><br>
 * tableName :the sqlDB's tableName that holds HEADER information <br>
 * indexName :ElasticSearch's indexName that want to verify <br>
 * logger    :Logger.class needed to acesses to sqlDB <br>
 * sleepTime :define sleepTime of one verification trigger check <br>
 * trigger   :when trigger == true, verification will starts
 */
public class BlockVerificationThread extends Thread {
	private boolean DEBUG = true;
	private String tableName;
	private String indexName;
	private Logger logger;

	public BlockVerificationThread(Logger logger, String indexName, String tableName){
		this.logger = logger;
		this.indexName = indexName;
		this.tableName = tableName;
	}

	@Override
	public void run(){
		if(DEBUG) System.err.println("BlockVerificationThread Start");
		try {
			if(DEBUG) System.err.println("trigger is HIGH, Starting verification");
			int latestHeaderNum = -1;
			int latestEsBlockNum = -1;
			try {
				latestHeaderNum = getLatestHeaderNum(tableName);
				if (DEBUG) System.err.println("latestHeaderNum :" + latestHeaderNum);
			}catch (SQLException e){
			}
			try {
				latestEsBlockNum = getLatestEsBlockNum(indexName);
				if(DEBUG) System.err.println("latestHeaderNum :"+latestHeaderNum);
			}catch (ElasticsearchException e){
			}
			int maxVerifyNum = (latestHeaderNum < latestEsBlockNum) ? latestHeaderNum -1 : latestEsBlockNum -1;
			if(maxVerifyNum < 0) {
				System.err.println("Block has not discovered, stop verifying");
				return;
			}
			verifyChain(maxVerifyNum);
			if(DEBUG) System.err.println("All verification PASS");
			//trigger = false;
		} catch (IOException | NoSuchFieldException | SQLException | EsRestClient.EsException | HeaderVerifyException | DataVerifyException e) {
			throw new Error(e);
		}
	}


	private int getLatestHeaderNum(String tableName) throws SQLException{
		String query = "SELECT max(idx) from " + tableName;
		var psmt = logger.getPreparedStatement(query);
		var rs = psmt.executeQuery();
		if (rs.next()) {
			return rs.getInt(1);
		} else {
			throw new SQLException("getLastHeaderNum fail");
		}
	}

	private int getLatestEsBlockNum(String indexName) throws NoSuchFieldException, IOException{
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();
		int max = esRestClient.getRightNextBlockNumber(indexName);
		esRestClient.disConnectToEs();
		return max;
	}

	private void verifyChain(int lastVerifyNum) throws SQLException, HeaderVerifyException, DataVerifyException, IOException, EsRestClient.EsException, NoSuchFieldException{
		for (int i = 0; i < lastVerifyNum + 1; i++) {
			verifyHeader(i);
			if(DEBUG) System.err.println("Blk#"+i+" HEADER PASS");
			verifyData(i);
			if(DEBUG) System.err.println("Blk#"+i+" ELASTIC PASS");
		}
	}

	private void verifyHeader(int i) throws SQLException, HeaderVerifyException{
		Triple<Integer, String, String> prevHeader = getHeader(i - 1);
		Triple<Integer, String, String> currHeader = getHeader(i);
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
					.append("checkList[0] :").append(checkList[0]).append("\n")
					.append("checkList[1] :").append(checkList[1]).append("\n")
					.append("checkList[2] :").append(checkList[1]).append("\n")
					.append("Header Broken, Call Admin and Check Data!!!!");

			StringBuilder errorCode = new StringBuilder();
			if (i == 0)
				errorCode.append("FIRST_HEADER, [ ");
			else
				errorCode.append("NON_FIRST_HEADER");

			errorCode.append(Arrays.stream(checkList).map(x -> x.toString() + "|"));
			errorCode.append(" ]");

			logger.insertErrorLog(System.currentTimeMillis(), i, -1, String.valueOf(errorCode));
			throw new HeaderVerifyException(exceptionMsg.toString());
		}
	}

	private void verifyData(int blockNum) throws NoSuchFieldException, SQLException, IOException, EsRestClient.EsException, DataVerifyException{
		if (blockNum == 0) return;
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		int entryNum = 0;
		String root = getHeader(blockNum).getMiddle();
		SecretKey key = makeSymmetricKey(root);
		List<Map<String, Object>> restoreMapList = new ArrayList<>();
		Pair<List<Map<String, Object>>, List<byte[]>> data = null;

		//try to decrpyt base64 encoding & decrypt encByte[] by generated key & deserialize decByte[] to Map.class
		try {
			data = esRestClient.getBlockDataPair(indexName, blockNum);
			for (entryNum = 0; entryNum < data.getRight().size(); entryNum++) {
				byte[] enc = data.getRight().get(entryNum);
				restoreMapList.add(desToObject(new String(decrypt(enc, key)), Map.class));
			}
		}catch (NoSuchFieldException e) {
			if(DEBUG) System.err.println(e.getMessage());
			entryNum = Integer.parseInt(e.getMessage());
			logger.insertErrorLog(System.currentTimeMillis(), blockNum, entryNum, "Entry_KeySet_Malformed");
			throw new DataVerifyException("BlockNum :" + blockNum + ", EntryNum :" + entryNum + " Entry_KeySet_Malformed");
		}
		catch (IllegalArgumentException e) {
			entryNum = Integer.parseInt(e.getMessage());
			logger.insertErrorLog(System.currentTimeMillis(), blockNum, entryNum, "Entry_Base64_Decoding_Fail");
			throw new DataVerifyException("BlockNum :" + blockNum + ", EntryNum :" + entryNum + " Entry_Base64_Decoding_Fail");
		} catch (EncryptionException e) {
			logger.insertErrorLog(System.currentTimeMillis(), blockNum, entryNum, "Entry_Decryption_Fail");
			throw new DataVerifyException("BlockNum :" + blockNum + ", EntryNum :" + entryNum + " Entry_Decryption_Fail");
		} catch (SerializationException s) {
			logger.insertErrorLog(System.currentTimeMillis(), blockNum, entryNum, "Entry_Deserialization_Fail");
			throw new DataVerifyException("BlockNum :" + blockNum + ", EntryNum :" + entryNum + " Entry_Deserialization_Fail");
		}

		//try to compare originalMap == restoreMap
		for (entryNum = 0; entryNum < data.getLeft().size(); entryNum++) {
			if (!isMapSame(data.getLeft().get(entryNum), restoreMapList.get(entryNum))) {
				logger.insertErrorLog(System.currentTimeMillis(), blockNum, entryNum, "Entry_Plain_NOT_SAME_WITH_RestoredPlain");
				throw new DataVerifyException("BlockNum :" + blockNum + ", EntryNum :" + entryNum + " Entry_Plain_NOT_SAME_WITH_RestoredPlain");
			}
		}

		//generate root' from restoreMap and compare root == root'
		String rootPrime = new HashTree(restoreMapList.stream()
				.map(x -> (Serializable) x)
				.map(Util::hash)
				.toArray(String[]::new))
				.toString();

		if (!root.equals(rootPrime)) {
			logger.insertErrorLog(System.currentTimeMillis(), blockNum, -1, "Root_NOT_SAME_WITH_RootPrime_OR_Missing_Document");
			throw new DataVerifyException("BlockNum :" + blockNum + ", EntryNum :" + entryNum + " Root_NOT_SAME_WITH_RootPrime_OR_Missing_Document");
		}
		logger.insertErrorLog(System.currentTimeMillis(), blockNum, data.getRight().size(), "ALL_PASS");
	}

	private Triple<Integer, String, String> getHeader(int headerNum) throws SQLException{
		if (headerNum < 0) {
			return null;
		}
		String query = "SELECT b.idx, b.root, b.prev from " + tableName + " AS b WHERE b.idx = " + (headerNum);
		var psmt = logger.getPreparedStatement(query);
		var rs = psmt.executeQuery();
		if (rs.next()) {
			return new ImmutableTriple<>(rs.getInt(1), rs.getString(2), rs.getString(3));
		} else {
			throw new SQLException("Table :" + tableName + " headerNum :" + headerNum + ", Can't Find Error");
		}
	}

	private boolean isMapSame(Map<String, Object> ori, Map<String, Object> res){
		if (ori.size() != res.size())
			return false;

		return ori.entrySet().stream()
				.allMatch(e -> e.getValue().equals(res.get(e.getKey())));
	}

	public static class HeaderVerifyException extends Exception {
		HeaderVerifyException(String s){
			super(s);
		}
	}

	public static class DataVerifyException extends Exception {
		DataVerifyException(String s){
			super(s);
		}
	}
}