package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;
import org.echocat.jsu.JdbcUtils;

import java.io.Serializable;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.diffplug.common.base.Errors.suppress;
import static kr.ac.hongik.apl.Util.desToObject;

public class ViewChangeMessage implements Message {
	private static Replica replica;
	final Data data;
	final byte[] signature;
	static private boolean DEBUG = true;
	private ViewChangeMessage(Data data, byte[] signature, Replica replica) {
		this.data = data;
		this.signature = signature;
		ViewChangeMessage.replica = replica;
	}

	public static ViewChangeMessage makeViewChangeMsg(int lastCheckpointNum, int newViewNum, Replica replica,
													  Function<String, PreparedStatement> preparedStatement) {
		if (DEBUG) {
			System.err.println("lastCheckpointNum : " + lastCheckpointNum + " newViewNum : " + newViewNum + " getMaximumFaulty : " + 1);//replica.getMaximumFaulty());
		}
		ViewChangeMessage.replica = replica;

		List<CheckPointMessage> checkPointMessages = getCheckpointMessages(preparedStatement, lastCheckpointNum,
				replica.getMaximumFaulty());
		List<Pm> messageList = getMessageList(preparedStatement, lastCheckpointNum, replica.getWatermarks(),
				replica.getMaximumFaulty());

		Data data = new Data(newViewNum, lastCheckpointNum, checkPointMessages, messageList, replica.getMyNumber());
		byte[] signature = Util.sign(replica.getPrivateKey(), data);
		if (DEBUG) {
			System.err.println("Checkpoint size : " + checkPointMessages.size() + " Pm size : " + messageList.size());
		}
		return new ViewChangeMessage(data, signature, replica);
	}

	private static List<Pm> getMessageList(Function<String, PreparedStatement> preparedStatement,
										   int checkpointNum, int[] waterMark,
										   int getMaximumFaulty) {

		List<Pm> pmList = new ArrayList<>();
		var watermarkRange = getWatermarkRange(checkpointNum, waterMark);

		for (int i : watermarkRange) {
			pmList.add(makePmOrNull(i, preparedStatement, getMaximumFaulty));
		}

		/*pmList = Arrays.stream(watermarkRange)
				.boxed()
				.map(suppress().wrapWithDefault(i -> makePmOrNull(i, preparedStatement, getMaximumFaulty), null))
				.collect(Collectors.toList());
		*/
		return pmList.stream().filter(Objects::nonNull).collect(Collectors.toList());
	}

	private static Pm makePmOrNull(int seqNum, Function<String, PreparedStatement> preparedStatement, int getMaximumFaulty) {


		PreprepareMessage preprepareMessage;
		List<PrepareMessage> prepareList;
		int maxViewNum;

		String query = "SELECT P1.digest, P1.data, P1.viewNum FROM Preprepares P1 " +
				"where P1.seqNum = ? and P1.viewNum = (SELECT MAX(viewNum) from Preprepares P2 where P2.seqNum = ?)";

		try (var pstmt = preparedStatement.apply(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.setInt(2, seqNum);
			var ret = pstmt.executeQuery();
			if (ret.next()) {
				maxViewNum = ret.getInt(3);
				preprepareMessage = desToObject(ret.getString(2), PreprepareMessage.class);
			} else
				return null;
		} catch (SQLException e) {
			return null;
		}

		query = "SELECT data FROM Prepares " +
				"WHERE seqNum = ? AND viewNum = ? GROUP BY digest HAVING count(*) > ? ";
		try (var pstmt = preparedStatement.apply(query)) {
			pstmt.setInt(1, seqNum);
			pstmt.setInt(2, maxViewNum);
			pstmt.setInt(3, 2 * getMaximumFaulty);
			try (var ret = pstmt.executeQuery()) {

				prepareList = JdbcUtils.toStream(ret)
						.map(suppress().wrapWithDefault(x -> x.getString(1), null))
						.filter(Objects::nonNull)
						.map(x -> desToObject(x, PrepareMessage.class))
						.collect(Collectors.toList());

			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		return prepareList.isEmpty() ? null : new Pm(preprepareMessage, prepareList);

	}

	private static int[] getWatermarkRange(int lastCheckpointNum, int[] waterMark) {
		int[] numList = IntStream.range(lastCheckpointNum, waterMark[1]).toArray();
		return numList;
	}

	private static List<CheckPointMessage> getCheckpointMessages(Function<String, PreparedStatement> preparedStatement, int checkpointNum, int getMaximumFaulty) {
		String query = "SELECT data FROM Checkpoints cp1 " +
				"where cp1.stateDigest = (select stateDigest from Checkpoints cp2 " +
				"WHERE seqNum = ? " +
				"GROUP BY stateDigest " +
				"HAVING count(*) > ?)";
		List<CheckPointMessage> checkpointList = null;

		try (var pstmt = preparedStatement.apply(query)) {
			pstmt.setInt(1, checkpointNum - 1);
			pstmt.setInt(2, 2 * getMaximumFaulty);
			try (var ret = pstmt.executeQuery()) {
				checkpointList = JdbcUtils.toStream(ret)
						.map(suppress().wrapWithDefault(x -> x.getString(1), null))
						.filter(Objects::nonNull)
						.map(x -> desToObject(x, CheckPointMessage.class))
						.collect(Collectors.toList());
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}

		return checkpointList;

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

	public boolean verify(PublicKey publicKey) {
		return Util.verify(publicKey, this.data, this.signature);
	}

	public boolean isVerified(PublicKey publicKey, int maximumFaulty, int WATERMARK_UNIT) {

		Boolean[] checkList = new Boolean[6];
		String replicaDigest = data.checkPointMessages.stream()
				.filter(x -> x.getReplicaNum() == data.replicaNum)
				.findFirst()
				.get()
				.getDigest();

		checkList[0] = verify(publicKey);

		//verify the set C has own checkPointMsg
		checkList[1] = data.checkPointMessages.stream().anyMatch(cpMsg -> cpMsg.getReplicaNum() == data.replicaNum);
		//verify the set C has over 2f+1 checkPointMsgs. that has same digest with (own checkPointMsg) & (lastCheckPointNum)
		checkList[2] = data.checkPointMessages.stream()
				.filter(cpMsg -> cpMsg.getDigest().equals(replicaDigest))
				.filter(cpMsg -> cpMsg.getSeqNum() == data.lastCheckpointNum - 1)
				.count() > 2 * maximumFaulty;

		//check messageList aren't bigger than WATERMARK_UNIT. !!!!need more verification!!!!
		checkList[3] = data.messageList.size() <= WATERMARK_UNIT;

		//check each Pm's prePareMsg has distinct replica number (i)
		checkList[4] = data.messageList.stream()
				.allMatch(pm -> pm.prepareMessages.stream()
						.filter(Util.distinctByKey(PrepareMessage::getReplicaNum))
						.count() == pm.prepareMessages.size());

		//check each Pm has valid prepareMsg for corresponding	pre-prepareMsg
		checkList[5] = data.messageList.stream()
				.filter(pm -> pm.prepareMessages.stream().allMatch(pre -> pre.getViewNum() == pm.preprepareMessage.getViewNum()))
				.filter(pm -> pm.prepareMessages.stream().allMatch(pre -> pre.getSeqNum() == pm.preprepareMessage.getSeqNum()))
				.count() == data.messageList.size();

		return Arrays.stream(checkList).allMatch(x -> x);
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
