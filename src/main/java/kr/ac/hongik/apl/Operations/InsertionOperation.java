package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.ES.EsRestClient;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.security.PublicKey;
import java.sql.SQLException;
import java.util.List;

public class InsertionOperation extends Operation {
	private List<byte[]> infoList;

	public InsertionOperation(PublicKey publickey, List<byte[]> infoList) {
		super(publickey);
		this.infoList = infoList;
	}

	@Override
	public Object execute(Object obj) {

		List<byte[][]> tripleList = preprocess();

		Pair<List<byte[]>, Integer> pair = storeToHeader(tripleList);

		List<byte[]> encryptedList = pair.getLeft();
		int blockNumber = pair.getRight();

		try {
			BulkResponse results = storeToES(encryptedList, blockNumber);

		}catch (Exception e){
			//deleteHeader();
		}

		return null;
	}

	private List<byte[][]> preprocess() {
		return null;
	}

	private Pair<List<byte[]>, Integer> storeToHeader(List<byte[][]> encryptedList) {
		return null;
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
