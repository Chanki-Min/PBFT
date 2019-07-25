package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Logger;
import org.apache.commons.lang3.tuple.Pair;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static kr.ac.hongik.apl.Util.*;

public class GetBlockOperation extends Operation{
	private int blockNumber;
	private static final String tableName = "BlockChain";

	public GetBlockOperation(PublicKey publicKey, int blockNumber){
		super(publicKey);
		this.blockNumber = blockNumber;
	}

	@Override
	public Object execute(Object obj) {
		try {
			Logger logger = (Logger) obj;
			return getDecryptedData(blockNumber, logger);

		}catch (IOException | EncryptionException | EsRestClient.EsException | NoSuchFieldException e) {
			throw new Error(e);
		}
	}

	private List<Map<String, Object>> getDecryptedData(int blockNumber, Logger logger) throws NoSuchFieldException, IOException, EsRestClient.EsException, EncryptionException{
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		List<Map<String, Object>> decryptedData = new ArrayList<>();
		Pair<List<Map<String, Object>>, List<byte[]>> pair = esRestClient.getBlockDataPair("block_chain", blockNumber, true);
		SecretKey key = getSecretKey(blockNumber, logger);
		if(key == null){
			throw new EncryptionException(new InvalidKeyException("key cannot be null"));
		}

		for(byte[] x: pair.getRight()){
			byte[] decryptDataByKey = decrypt(x, key);
			decryptedData.add(desToObject(new String(decryptDataByKey), Map.class));
		}

		esRestClient.disConnectToEs();
		if(!isListMapSame(pair.getLeft(), decryptedData)){
			return null;
		}
		return decryptedData;
	}

	private SecretKey getSecretKey(int blockNumber, Logger logger) {
		String query = "SELECT root FROM " + tableName + " b WHERE b.idx = ?";
		try (var pstmt = logger.getPreparedStatement(query)) {
			String root = null;
			pstmt.setInt(1, blockNumber);

			var ret = pstmt.executeQuery();
			if (ret.next()) {
				root = ret.getString(1);
			}
			if(root == null) {
				return null;
			}
			return makeSymmetricKey(root);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	private boolean isListMapSame(List<Map<String, Object>> ori, List<Map<String, Object>> res){
		if(ori.size() != res.size())
			return false;

		return IntStream.range(0, ori.size())
				.allMatch(i -> ori.get(i).entrySet().stream()
						.allMatch(e -> e.getValue().equals(res.get(i).get(e.getKey()))));
	}
}
