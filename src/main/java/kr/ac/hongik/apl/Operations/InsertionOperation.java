package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.Blockchain.BlockVerificationThread;
import kr.ac.hongik.apl.Blockchain.HashTree;
import kr.ac.hongik.apl.ES.EsJsonParser;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.elasticsearch.ElasticsearchCorruptionException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.Serializable;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Thread.sleep;
import static kr.ac.hongik.apl.Util.*;

public class InsertionOperation extends Operation {
	private final boolean DEBUG = false;
	private final String indexName = "block_chain";
	private final String tableName = "BlockChain";
	//value for bulkInsertProcessor
	private final int maxAction = 100;
	private final int maxSize = 10;
	private final ByteSizeUnit maxSizeUnit = ByteSizeUnit.MB;
	private final int threadSize = 5;

	private final int sleepTime = 3000;
	private List<Map<String, Object>> infoList;


	public InsertionOperation(PublicKey publickey, List<Map<String, Object>> infoList) {
		super(publickey);
		this.infoList = infoList;
	}

	/**
	 * @param obj
	 * @return Integer value of inserted BlockNumber#
	 */
	@Override
	public Object execute(Object obj) {
		List<byte[]> encryptedList = null;
		int blockNumber = 0;
		try {
			Logger logger = (Logger) obj;
			createTableIfNotExists(logger);

			Pair<List<byte[]>, Integer> pair = storeHeaderAndReturnData(infoList, logger);

			encryptedList = pair.getLeft();
			blockNumber = pair.getRight();

			storeToES(infoList, encryptedList, blockNumber);
		} catch (EncryptionException | NoSuchFieldException | IOException | SQLException | EsRestClient.EsException | InterruptedException | EsRestClient.EsSSLException e) {
			throw new Error(e);
		} catch (EsRestClient.EsConcurrencyException ignored) {
		}
		//verification stack
		try {
			sleep(sleepTime);
			if (checkEsBlockAndUpdateWhenWrong(indexName, blockNumber, infoList, encryptedList))
				return blockNumber;
			else
				throw new EsRestClient.EsException("checkEsBlockAndUpdate failed");
		} catch (NoSuchFieldException | EsRestClient.EsException | IOException | InterruptedException | EsRestClient.EsSSLException e) {
			System.err.print(e.getMessage());
			throw new Error(e);
		} finally {
			//if block get to VERIFY_UNIT, start Verification thread
			if (blockNumber > 0 && (blockNumber % Replica.VERIFY_UNIT) == 0) {
				Thread thread = new BlockVerificationThread((Logger) obj, indexName, tableName);
				thread.start();
			}
		}
	}

	/**
	 * createTableIfNotExists creates a BlockChain table and insert a dummy row.
	 *
	 * @param logger
	 */
	private void createTableIfNotExists(Logger logger) {
		try {
			logger.getPreparedStatement("CREATE TABLE " + tableName + " (idx INT, root TEXT, prev TEXT, PRIMARY KEY (idx, root, prev)) ").execute();
			logger.getPreparedStatement("INSERT INTO " + tableName + " VALUES (0, 'FIRST_ROOT', 'PREV')").execute();
		} catch (SQLException ignored) {
		}
	}

	/**
	 * @param infoList
	 * @param logger
	 * @return A pair of (encrypted entry list, its block number)
	 * @throws Util.EncryptionException
	 * @throws SQLException
	 */
	private Pair<List<byte[]>, Integer> storeHeaderAndReturnData(List<Map<String, Object>> infoList, Logger logger) throws Util.EncryptionException, SQLException {
		HashTree tree = new HashTree(infoList.stream().map(x -> (Serializable) x).map(Util::hash).toArray(String[]::new));
		String root = tree.toString();
		SecretKey key = makeSymmetricKey(root);

		List<byte[]> encryptedList = new ArrayList<>();

		for (Map<String, Object> x: infoList) {
			byte[] encrypt = encrypt(Util.serToString((Serializable) x).getBytes(), key);
			encryptedList.add(encrypt);
		}
		int idx = storeHeaderAndReturnIdx(root, logger);

		return new ImmutablePair<>(encryptedList, idx);
	}

	/**
	 * @param root   root is a root of merkle tree
	 * @param logger
	 * @return An index of just inserted block.
	 * @throws SQLException
	 */
	private int storeHeaderAndReturnIdx(String root, Logger logger) throws SQLException {
		String query = "INSERT INTO " + tableName + " VALUES ( ?, ?, ? )";
		try (var psmt = logger.getPreparedStatement(query)) {
			Triple<Integer, String, String> previousBlock = getLatestBlock(logger);
			String prevHash = Util.hash(previousBlock.toString());

			psmt.setInt(1, previousBlock.getLeft() + 1);
			psmt.setString(2, root);
			psmt.setString(3, prevHash);

			psmt.execute();

			return previousBlock.getLeft() + 1;
		}
	}

	/**
	 * @param logger
	 * @return A triplet of (Block number, merkle root, previous block hash)
	 * @throws SQLException
	 */
	private Triple<Integer, String, String> getLatestBlock(Logger logger) throws SQLException {
		String query = "SELECT idx, root, prev FROM " + tableName + " b WHERE b.idx = (SELECT MAX(idx) from " + tableName + " b2)";
		try (var psmt = logger.getPreparedStatement(query)) {
			var ret = psmt.executeQuery();
			if (ret.next())
				return new ImmutableTriple<>(ret.getInt(1), ret.getString(2), ret.getString(3));
			else
				throw new SQLException("There's no tuple in " + tableName);
		}

	}

	/**
	 * @param encryptList
	 * @param blockNumber
	 * @return ElasticSearch's BulkResponse instance of  data-insertion
	 * @throws NoSuchFieldException
	 * @throws IOException
	 * @throws EsRestClient.EsException
	 * @throws EsRestClient.EsConcurrencyException throws when other replica already inserting (Indexes or Documents) that has same (indexName,blockNumber,versionNumber)
	 */
	private void storeToES(List<Map<String, Object>> plainDataList, List<byte[]> encryptList, int blockNumber) throws NoSuchFieldException, IOException, EsRestClient.EsException, EsRestClient.EsConcurrencyException, InterruptedException, EsRestClient.EsSSLException {
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		if (!esRestClient.isIndexExists("block_chain")) {
			try {
				EsJsonParser parser = new EsJsonParser();
				XContentBuilder mappingBuilder;
				XContentBuilder settingBuilder;

				parser.setFilePath("/ES_MappingAndSetting/ES_mapping_with_plain.json");
				mappingBuilder = parser.jsonFileToXContentBuilder(false);

				parser.setFilePath("/ES_MappingAndSetting/ES_setting_with_plain.json");
				settingBuilder = parser.jsonFileToXContentBuilder(false);
				esRestClient.createIndex("block_chain", mappingBuilder, settingBuilder);
			} catch (ElasticsearchCorruptionException e) {
				System.err.println(e.getMessage());
			}
		}

		esRestClient.bulkInsertDocumentByProcessor(
				"block_chain", blockNumber, plainDataList, encryptList, 1, maxAction, maxSize, maxSizeUnit, threadSize);
		esRestClient.disConnectToEs();
	}

	/**
	 * Get Encrypted blockData from Es, and compare with original data. when Es-data isn't equals with original-data, update block with origin-data with currVersionNumber+1
	 *
	 * @param indexName
	 * @param blockNumber
	 * @param encryptList List<> of encrypted blockData that originally intended to insert in Es
	 * @return encryptList.equals(esRestClient.getBlockDataPair ( indexName, blockNumber))
	 * @throws NoSuchFieldException
	 * @throws IOException
	 * @throws EsRestClient.EsException
	 */
	private boolean checkEsBlockAndUpdateWhenWrong(String indexName, int blockNumber, List<Map<String, Object>> plainDataList, List<byte[]> encryptList) throws NoSuchFieldException, IOException, EsRestClient.EsException, EsRestClient.EsSSLException {
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();
		if (DEBUG) {
			System.err.print("InsertionOp::checkEsBlolck::Ori :[ ");
			encryptList.stream()
					.forEach(x -> System.err.print(hash(x).substring(0, 5) + ", "));
			System.err.println(" ]");
			System.err.print("InsertionOp::checkEsBlolck::Res :[ ");
			esRestClient.getBlockDataPair(indexName, blockNumber).getRight().stream()
					.forEach(x -> System.err.print(hash(x).substring(0, 5) + ", "));
			System.err.println(" ]");
			System.err.println("InsertionOp::isSame? :" + esRestClient.isDataEquals(encryptList, esRestClient.getBlockDataPair(indexName, blockNumber).getRight()));
		}


		if (esRestClient.isDataEquals(encryptList, esRestClient.getBlockDataPair(indexName, blockNumber).getRight()))
			return true;

		long currHeadDocVersion = esRestClient.getDocumentVersion(indexName, blockNumber, -1);
		try {
			esRestClient.bulkInsertDocumentByProcessor(
					"block_chain", blockNumber, plainDataList, encryptList, currHeadDocVersion + 1, maxAction, maxSize, maxSizeUnit, threadSize);
		} catch (EsRestClient.EsConcurrencyException | InterruptedException ignore) {
		}
		return esRestClient.isDataEquals(encryptList, esRestClient.getBlockDataPair(indexName, blockNumber).getRight());
	}
}
