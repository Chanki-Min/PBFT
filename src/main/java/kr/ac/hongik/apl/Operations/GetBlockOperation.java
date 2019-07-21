package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.ES.EsRestClient;
import kr.ac.hongik.apl.Logger;
import org.apache.commons.lang3.tuple.Triple;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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

	private List<byte[]> getDecryptedData(int blockNumber, Logger logger) throws NoSuchFieldException, IOException, EsRestClient.EsException, EncryptionException{
		EsRestClient esRestClient = new EsRestClient();
		esRestClient.connectToEs();

		List<byte[]> originalData = new ArrayList<>();
		List<byte[]> encryptedData = esRestClient.getBlockByteArray("block_chain", blockNumber);
		SecretKey key = getSecretKey(blockNumber, logger);
		if(key == null){
			throw new EncryptionException(new InvalidKeyException("key cannot be null"));
		}

		for(byte[] x: encryptedData){
			byte[] decryptDataByKey = decrypt(x, key);
			originalData.add(desToObject(new String(decryptDataByKey), byte[].class) );
		}

		esRestClient.disConnectToEs();
		return originalData;
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
}
