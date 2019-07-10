package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Util;
import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static kr.ac.hongik.apl.Util.desToObject;

public class ViewChangeMessage implements Message {
	final Data data;
	final byte[] signature;


	private ViewChangeMessage(Data data, byte[] signature) {
		this.data = data;
		this.signature = signature;
	}

	public static ViewChangeMessage makeViewChangeMsg(PrivateKey privateKey, int lastCheckpointNum, int newViewNum, int replicaNum,
													  Function<String, PreparedStatement> preparedStatement) {

		List<CheckPointMessage> checkPointMessages = getCheckpointMessages(preparedStatement, lastCheckpointNum);
		List<Pm> messageList = getMessageList(privateKey, preparedStatement, lastCheckpointNum);

		Data data = new Data(newViewNum, lastCheckpointNum, checkPointMessages, messageList, replicaNum);
		byte[] signature = Util.sign(privateKey, data);

		return new ViewChangeMessage(data, signature);
	}

	public boolean isVerified(PublicKey publicKey, int maximumFaulty, int WATERMARK_UNIT){

		Boolean[] checkList = new Boolean[6];
		String replicaDigest = data.checkPointMessages.stream()
				.filter(x -> x.getReplicaNum() == data.replicaNum)
				.findFirst()
				.get()
				.getDigest();

		checkList[0] = Util.verify(publicKey, this.data, this.signature);

		//verify the set C has own checkPointMsg
		checkList[1] = data.checkPointMessages.stream().anyMatch(cpMsg -> cpMsg.getReplicaNum() == data.replicaNum);
		//verify the set C has over 2f+1 checkPointMsgs. that has same digest with (own checkPointMsg) & (lastCheckPointNum)
		checkList[2] = data.checkPointMessages.stream()
				.filter(cpMsg -> cpMsg.getDigest() == replicaDigest)
				.filter(cpMsg -> cpMsg.getSeqNum() == data.lastCheckpointNum -1)
				.count() > 2 * maximumFaulty;

		//check messageList aren't bigger than WATERMARK_UNIT. !!!!need more verification!!!!
		checkList[3] = data.messageList.size() <= WATERMARK_UNIT;

		//check each Pm's prePareMsg has distinct replica number (i)
		checkList[4] = data.messageList.stream()
				.allMatch(pm -> pm.prepareMessages.stream()
					.filter( Util.distinctByKey(preMsg -> preMsg.getReplicaNum()) )
				.count() == pm.prepareMessages.size());

		//check each Pm has valid prepareMsg for corresponding	pre-prepareMsg
		checkList[5] = data.messageList.parallelStream()
				.filter(pm -> pm.prepareMessages.stream().allMatch(pre-> pre.getViewNum() == pm.preprepareMessage.getViewNum()))
				.filter(pm -> pm.prepareMessages.stream().allMatch(pre-> pre.getSeqNum() == pm.preprepareMessage.getSeqNum()))
				.filter(pm -> pm.prepareMessages.stream().allMatch(pre-> pre.getDigest() == pm.preprepareMessage.getDigest()))
				.collect(Collectors.toList()).size() == data.messageList.size();
		return Arrays.stream(checkList).allMatch(x -> x);
	}



	public int getNewViewNum() {
		return data.newViewNum;
	}

	public int getLastCheckpointNum() {
		return data.lastCheckpointNum;
	}

	public List<CheckPointMessage> getCheckPointMessages() {
		return data.checkPointMessages;
	}

	public List<Pm> getMessageList() {
		return data.messageList;
	}

	public int getReplicaNum() {
		return data.replicaNum;
	}

	private static List<Pm> getMessageList(PrivateKey privateKey, Function<String, PreparedStatement> preparedStatement,
										   int checkpointNum) {

		String query = "SELECT viewNum,seqNum,digest,operation FROM Preprepares ";
		PreparedStatement pstmt = preparedStatement.apply(query);
		List<PreprepareMessage> preprepareList = new ArrayList<>();

		try (var ret = pstmt.executeQuery()) {
			while (ret.next()) {

				String data = ret.getString(4);
				Operation operation = desToObject(data, Operation.class);
				PreprepareMessage preprepareMessage = PreprepareMessage.makePrePrepareMsg(
						privateKey,
						ret.getInt(1),
						ret.getInt(2),
						operation);
				preprepareList.add(preprepareMessage);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}


		/*throw new NotImplementedException("checkpointNum 보다 크커나 같은 각각의 sequence number n에 대하여 " +
				"n에 해당하는 PrePrepareMessage, " +
				"n에 해당하는 prepare message들, " +
				"그리고 D(request)을 P_n이라고 한다. 이 P_n들의 리스트를 반환해야 한다.");*/
		return;
	}


	private static List<CheckPointMessage> getCheckpointMessages(Function<String, PreparedStatement> preparedStatement, int checkpointNum) {
		//TODO
		throw new NotImplementedException("checkpointNum(Watermark[0])에 해당하는 checkpoint message들을 반환해야 함");
	}

	private static class Data implements Serializable {
		private final int newViewNum;
		private final int lastCheckpointNum;
		private final List<CheckPointMessage> checkPointMessages;
		private final List<Pm> messageList;
		private final int replicaNum;

		private Data(int newViewNum, int lastCheckpoint, List<CheckPointMessage> checkPointMessages, List<Pm> pmList, int replicaNum) {
			this.newViewNum = newViewNum;
			this.lastCheckpointNum = lastCheckpoint;
			this.checkPointMessages = checkPointMessages;
			this.messageList = pmList;
			this.replicaNum = replicaNum;
		}


	}

	static class Pm implements Serializable {
		private final PreprepareMessage preprepareMessage;
		private final List<PrepareMessage> prepareMessages;
		private final String requestDigest;

		private Pm(PreprepareMessage preprepareMessage, List<PrepareMessage> prepareMessages, String requestDigest) {
			this.preprepareMessage = preprepareMessage;
			this.prepareMessages = prepareMessages;
			this.requestDigest = requestDigest;
		}

		public PreprepareMessage getPreprepareMessage() {
			return preprepareMessage;
		}

		public List<PrepareMessage> getPrepareMessages() {
			return prepareMessages;
		}

		public String getRequestDigest() {
			return requestDigest;
		}
	}
}
