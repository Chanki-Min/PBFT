package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.Blockchain.HashTree;
import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Logger;
import kr.ac.hongik.apl.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.elasticsearch.action.bulk.BulkResponse;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;
import static kr.ac.hongik.apl.Util.*;

public class InsertionOperation extends Operation {
	private final boolean DEBUG = true;
	private final int sleepTime = 3000;
	private List<byte[]> infoList;
	private static final String tableName = "BlockChain";

	public InsertionOperation(PublicKey publickey, List<byte[]> infoList) {
		super(publickey);
		this.infoList = infoList;
	}

	@Override
	public Object execute(Object obj) {
		List<byte[]> encryptedList = null;
		int blockNumber = 0;
		try {
			Logger logger = (Logger) obj;
			createTableIfNotExists(logger);

			List<Triple<byte[], byte[], byte[]>> tripleList = preprocess();

			Pair<List<byte[]>, Integer> pair = storeHeaderAndReturnData(tripleList, logger);

			encryptedList = pair.getLeft();
			blockNumber = pair.getRight();

			storeToES(encryptedList, blockNumber);
		} catch (Util.EncryptionException | NoSuchFieldException | IOException | SQLException | EsRestClient.EsException e) {
			throw new Error(e);
		} catch (EsRestClient.EsConcurrencyException ignored) {
		}

		//verification stack
		try {
			sleep(sleepTime);
			checkEsBlockAndUpdateWhenWrong("block_chain",blockNumber,encryptedList);
			return true;
		} catch (NoSuchFieldException | EsRestClient.EsException | IOException | InterruptedException e) {
			System.err.print(e.getMessage());
			throw new Error(e);
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
	 * @return A list of (previous item, current item, next item).
	 * If current item is at the edge, then a side item will be null.
	 */
	private List<Triple<byte[], byte[], byte[]>> preprocess() {

		return IntStream.range(0, infoList.size())
				.mapToObj(i -> new ImmutableTriple<>(
						i - 1 >= 0 ? infoList.get(i - 1) : null,
						infoList.get(i),
						i + 1 < infoList.size() ? infoList.get(i + 1) : null))
				.collect(Collectors.toList());
	}

	/**
	 * @param tripleList
	 * @param logger
	 * @return A pair of (encrypted entry list, its block number)
	 * @throws Util.EncryptionException
	 * @throws SQLException
	 */
	private Pair<List<byte[]>, Integer> storeHeaderAndReturnData(List<Triple<byte[], byte[], byte[]>> tripleList, Logger logger) throws Util.EncryptionException, SQLException {
		HashTree tree = new HashTree(tripleList.stream().map(Util::hash).toArray(String[]::new));
		String root = tree.toString();
		SecretKey key = makeSymmetricKey(root);

		List<byte[]> encryptedList = new ArrayList<>();

		for (Triple<byte[], byte[], byte[]> x : tripleList) {
			byte[] encrypt = encrypt(Util.serToString(x).getBytes(), key);
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
		String query = "SELECT idx, root, prev FROM " + tableName + " b WHERE b.idx = (SELECT MAX(idx) from "+tableName+ " b2)";
		try (var psmt = logger.getPreparedStatement(query)) {
			var ret = psmt.executeQuery();
			if (ret.next())
				return new ImmutableTriple<>(ret.getInt(1), ret.getString(2), ret.getString(3));
			else
				throw new SQLException("There's no tuple in " + tableName);
		}

	}


	/**
	 * @param encryptedList
	 * @param blockNumber
	 * @return ElasticSearch's BulkResponse instance of  data-insertion
	 * @throws NoSuchFieldException
	 * @throws IOException
	 * @throws EsRestClient.EsException
	 * @throws EsRestClient.EsConcurrencyException throws when other replica already inserting (Indexes or Documents) that has same (indexName,blockNumber,versionNumber)
	 */
	private BulkResponse storeToES(List<byte[]> encryptedList, int blockNumber) throws NoSuchFieldException, IOException, EsRestClient.EsException, EsRestClient.EsConcurrencyException{
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		if(!esRestClient.isIndexExists("block_chain")){
			esRestClient.createIndex("block_chain");
		}

		BulkResponse bulkResponse = esRestClient.bulkInsertDocument("block_chain", blockNumber, encryptedList, 1);
		esRestClient.disConnectToEs();
		return bulkResponse;
	}

	/**
	 * Get Encrypted blockData from Es, and compare with original data. when Es-data isn't equals with original-data, update block with origin-data with currVersionNumber+1
	 * @param indexName
	 * @param blockNumber
	 * @param encryptList List<> of encrypted blockData that originally intended to insert in Es
	 * @return encryptList.equals(esRestClient.getBlockByteArray(indexName, blockNumber))
	 * @throws NoSuchFieldException
	 * @throws IOException
	 * @throws EsRestClient.EsException
	 */
	private boolean checkEsBlockAndUpdateWhenWrong(String indexName, int blockNumber, List<byte[]> encryptList) throws NoSuchFieldException, IOException, EsRestClient.EsException{
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();
		if(DEBUG){
			System.err.print("InsertionOp::checkEsBlolck::Ori :[ "); encryptList.stream()
					.forEach(x -> System.err.print(hash(x).substring(0,10)+", "));
			System.err.println(" ]");
			System.err.print("InsertionOp::checkEsBlolck::Res :[ "); esRestClient.getBlockByteArray(indexName, blockNumber).stream()
					.forEach(x -> System.err.print(hash(x).substring(0,10)+", "));
			System.err.println(" ]");
			System.err.println("InsertionOp::isSame? :"+esRestClient.isDataEquals(encryptList, esRestClient.getBlockByteArray(indexName, blockNumber)));
		}

		if (esRestClient.isDataEquals(encryptList, esRestClient.getBlockByteArray(indexName, blockNumber)))
			return true;

		long currHeadDocVersion = esRestClient.getDocumentVersion(indexName, blockNumber, -1);
		try {
			esRestClient.bulkInsertDocument(indexName, blockNumber, encryptList, currHeadDocVersion + 1);
		}catch (EsRestClient.EsConcurrencyException ignore) {
		}
		return esRestClient.isDataEquals(encryptList, esRestClient.getBlockByteArray(indexName, blockNumber));

	}




}
