package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Util;

import java.io.Serializable;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
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
													  Function<String, PreparedStatement> preparedStatement,
													  Supplier<int[]> waterMarkGet) throws SQLException {

		List<CheckPointMessage> checkPointMessages = getCheckpointMessages(preparedStatement, lastCheckpointNum);
		List<Pm> messageList = getMessageList(preparedStatement, lastCheckpointNum, newViewNum, waterMarkGet);

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


	private static List<Pm> getMessageList(Function<String, PreparedStatement> preparedStatement,
										   int checkpointNum, int newViewNum, Supplier<int[]> waterMarkGet) throws SQLException {
		List<Pm> pmList = new ArrayList<Pm>();
		List<Integer> n = getrange(checkpointNum, waterMarkGet);

		for (int i = 0; i < n.size(); i++) {
			pmList.add(makePmOne(n.get(i), preparedStatement, newViewNum));
		}

		/*throw new NotImplementedException
		("checkpointNum 보다 크커나 같은 각각의 sequence number n에 대하여 " +
				"n에 해당하는 PrePrepareMessage, " +
				"n에 해당하는 prepare message들, " +
				"그리고 D(request)을 P_n이라고 한다. 이 P_n들의 리스트를 반환해야 한다.");*/
		return pmList;
	}

	private static Pm makePmOne(int seqNum, Function<String, PreparedStatement> preparedStatement, int newViewNum) throws SQLException {

		PreprepareMessage preprepareMessage;
		List<PrepareMessage> prepareList = new ArrayList<PrepareMessage>();
		String query = "SELECT digest,data FROM Preprepares WHERE seqNum = ? AND viewNum = ?";

		try (var pstmt = preparedStatement.apply(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.setInt(2, newViewNum - 1);
			var ret = pstmt.executeQuery();
			preprepareMessage = desToObject(ret.getString(2), PreprepareMessage.class);
		}

		query = "SELECT data FROM Prepares WHERE seqNum = ? AND viewNum = ? AND digest = ?";
		try (var pstmt = preparedStatement.apply(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.setInt(2, newViewNum - 1);
			pstmt.setString(3, preprepareMessage.getDigest());
			try (var ret = pstmt.executeQuery()) {
				while (ret.next()) {
					PrepareMessage prepareMessage = desToObject(ret.getString(1), PrepareMessage.class);
					prepareList.add(prepareMessage);
				}
			}
		}
		return new Pm(preprepareMessage, prepareList);

	}

	private static List<Integer> getrange(int lastCheckpointNum, Supplier<int[]> waterMarkGet) {
		List<Integer> n = new ArrayList<Integer>();
		int[] watermarks = waterMarkGet.get();
		for (int i = lastCheckpointNum; i < watermarks[1]; i++)
			n.add(i);
		return n;
	}
	private static List<CheckPointMessage> getCheckpointMessages(Function<String, PreparedStatement> preparedStatement, int checkpointNum) {
		//TODO
		String query = "SELECT data FROM Checkpoints WHERE seqnum <= ?";
		List<CheckPointMessage> checkpointList = new ArrayList<>();

		try (var pstmt = preparedStatement.apply(query)) {
			pstmt.setInt(1, checkpointNum);
			try (var ret = pstmt.executeQuery()) {
				while (ret.next()) {
					CheckPointMessage checkPointMessage = desToObject(ret.getString(1), CheckPointMessage.class);
					checkpointList.add(checkPointMessage);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return checkpointList;

		//throw new NotImplementedException("checkpointNum(Watermark[0])에 해당하는 checkpoint message들을 반환해야 함");
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

		private Pm(PreprepareMessage preprepareMessage, List<PrepareMessage> prepareMessages) {
			this.preprepareMessage = preprepareMessage;
			this.prepareMessages = prepareMessages;

		}

		public PreprepareMessage getPreprepareMessage() {
			return preprepareMessage;
		}

		public List<PrepareMessage> getPrepareMessages() {
			return prepareMessages;
		}


	}
}
