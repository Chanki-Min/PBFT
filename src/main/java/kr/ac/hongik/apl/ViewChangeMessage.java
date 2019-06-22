package kr.ac.hongik.apl;

import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.security.PrivateKey;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.function.Function;

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
		List<Pm> messageList = getMessageList(preparedStatement, lastCheckpointNum);

		Data data = new Data(newViewNum, lastCheckpointNum, checkPointMessages, messageList, replicaNum);
		byte[] signature = Util.sign(privateKey, data);

		return new ViewChangeMessage(data, signature);
	}

	private static List<Pm> getMessageList(Function<String, PreparedStatement> preparedStatement, int checkpointNum) {
		//TODO
		throw new NotImplementedException("checkpointNum 보다 큰 각각의 sequence number n에 대하여 " +
				"n에 해당하는 PrePrepareMessage, " +
				"n에 해당하는 prepare message들, " +
				"그리고 D(request)을 P_n이라고 한다. 이 P_n들의 리스트를 반환해야 한다.");
	}

	private static List<CheckPointMessage> getCheckpointMessages(Function<String, PreparedStatement> preparedStatement, int checkpointNum) {
		//TODO
		throw new NotImplementedException("checkpointNum에 해당하는 checkpoint message들을 반환해야 함");
	}

	private static class Data implements Serializable {
		final int newViewNum;
		final int lastCheckpointNum;
		final List<CheckPointMessage> checkPointMessages;
		final List<Pm> messageList;
		final int replicaNum;

		private Data(int newViewNum, int lastCheckpoint, List<CheckPointMessage> checkPointMessages, List<Pm> pmList, int replicaNum) {
			this.newViewNum = newViewNum;
			this.lastCheckpointNum = lastCheckpoint;
			this.checkPointMessages = checkPointMessages;
			this.messageList = pmList;
			this.replicaNum = replicaNum;
		}
	}

	private static class Pm implements Serializable {
		final PreprepareMessage preprepareMessage;
		final List<PrepareMessage> prepareMessages;
		final String requestDigest;

		private Pm(PreprepareMessage preprepareMessage, List<PrepareMessage> prepareMessages, String requestDigest) {
			this.preprepareMessage = preprepareMessage;
			this.prepareMessages = prepareMessages;
			this.requestDigest = requestDigest;
		}
	}
}
