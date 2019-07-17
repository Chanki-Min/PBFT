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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static kr.ac.hongik.apl.Util.encrypt;
import static kr.ac.hongik.apl.Util.makeSymmetricKey;

public class InsertionOperation extends Operation {
	private List<byte[]> infoList;
	private static final String tableName = "BlockChain";

	public InsertionOperation(PublicKey publickey, List<byte[]> infoList) {
		super(publickey);
		this.infoList = infoList;
	}

	@Override
	public Object execute(Object obj) {
		try {
			Logger logger = (Logger) obj;
			createTableIfNotExists(logger);

			List<Triple<byte[], byte[], byte[]>> tripleList = preprocess();

			Pair<List<byte[]>, Integer> pair = storeHeaderAndReturnData(tripleList, logger);

			List<byte[]> encryptedList = pair.getLeft();
			int blockNumber = pair.getRight();

			BulkResponse results = storeToES(encryptedList, blockNumber);

		} catch (Util.EncryptionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (NoSuchFieldException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * createTableIfNotExists creates a BlockChain table and insert a dummy row.
	 *
	 * @param logger
	 */
	private void createTableIfNotExists(Logger logger) {
		try {
			logger.getPreparedStatement("CREATE TABLE " + tableName + " (idx INT, root TEXT, prev TEXT, PRIMARY KEY (idx, root, prev)").execute();
			logger.getPreparedStatement("INSERT INTO " + tableName + "VALUES (0, FIRST_ROOT, PREV)").execute();
		} catch (SQLException ignored) {
		}

	}

	/**
	 * @return A list of (previous item, current item, next item).
	 * If current item is at the edge, then a side item will be null.
	 */
	private List<Triple<byte[], byte[], byte[]>> preprocess() {
		List<Optional<byte[]>> input = infoList.stream().map(Optional::of).collect(Collectors.toList());

		return IntStream.range(0, infoList.size())
				.mapToObj(i -> new ImmutableTriple<>(
						input.get(i - 1).orElse(null),
						input.get(i).get(),
						input.get(i + 1).orElse(null)))
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
		String query = "INSERT INTO " + tableName + "VALUES ( ?, ?, ? )";
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
		String query = "SELECT MAX(idx) FROM " + tableName;
		try (var psmt = logger.getPreparedStatement(query)) {
			var ret = psmt.executeQuery();
			if (ret.next())
				return new ImmutableTriple<>(ret.getInt(1), ret.getString(2), ret.getString(3));
			else
				throw new SQLException("There's no tuple in " + tableName);
		}

	}


	private BulkResponse storeToES(List<byte[]> encryptedList, int blockNumber) throws NoSuchFieldException, IOException, SQLException{
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		if(!esRestClient.isIndexExists("block_chain")){
			XContentBuilder mappingBuilder = new XContentFactory().jsonBuilder();
			mappingBuilder.startObject();
			{
				mappingBuilder.startObject("properties");
				{
					mappingBuilder.startObject("block_number");
					{
						mappingBuilder.field("type", "long");
						mappingBuilder.field("coerce", false);            //forbid auto-casting String to Integer
						mappingBuilder.field("ignore_malformed", true);    //forbid non-numeric values
					}
					mappingBuilder.endObject();

					mappingBuilder.startObject("entry_number");
					{
						mappingBuilder.field("type", "long");
						mappingBuilder.field("coerce", false);
						mappingBuilder.field("ignore_malformed", true);
					}
					mappingBuilder.endObject();

					mappingBuilder.startObject("encrypt_data");
					{
						mappingBuilder.field("type", "binary");
					}
					mappingBuilder.endObject();
				}
				mappingBuilder.endObject();
				mappingBuilder.field("dynamic", "strict");    //forbid auto field creation
			}
			mappingBuilder.endObject();

			XContentBuilder settingsBuilder = new XContentFactory().jsonBuilder();
			settingsBuilder.startObject();
			{
				settingsBuilder.field("index.number_of_shards", 4);
				settingsBuilder.field("index.number_of_replicas", 3);
				settingsBuilder.field("index.merge.scheduler.max_thread_count", 1);
			}
			settingsBuilder.endObject();
			esRestClient.createIndex("test_block_chain",mappingBuilder,settingsBuilder);
		}

		esRestClient.bulkInsertDocument("block_chain", blockNumber, encryptedList, 1);

		esRestClient.disConnectToEs();


		return null;
	}
}
