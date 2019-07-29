package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;
import org.echocat.jsu.JdbcUtils;

import java.io.Serializable;
import java.nio.channels.SocketChannel;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.diffplug.common.base.Errors.rethrow;
import static kr.ac.hongik.apl.Messages.PreprepareMessage.makePrePrepareMsg;
import static kr.ac.hongik.apl.Replica.DEBUG;

public class NewViewMessage implements Message {
	private final Data data;
	private final byte[] signature;


	private NewViewMessage(Data data, byte[] signature) {
		this.data = data;
		this.signature = signature;
	}

	public static NewViewMessage makeNewViewMessage(Replica replica, int newViewNum) throws SQLException {
		Function<String, PreparedStatement> queryFn = rethrow().wrap(replica.getLogger()::getPreparedStatement);
		List<ViewChangeMessage> viewChangeMessages = getViewChangeMessages(queryFn, newViewNum);    //GC가 이미 끝나서 DB안에는 last checkpoint 이후만 있다고 가정
		List<PreprepareMessage> operationList = getOperationList(replica, viewChangeMessages, newViewNum);
		if (DEBUG) {
			System.err.print("making new view message operationList size : ");
			if (operationList == null)
				System.err.println("null");
			else
				System.err.println(operationList.size());
		}
		Data data = new Data(newViewNum, viewChangeMessages, operationList);
		byte[] signature = Util.sign(replica.getPrivateKey(), data);


		return new NewViewMessage(data, signature);
	}

	private static List<PreprepareMessage> getOperationList(Replica replica, List<ViewChangeMessage> viewChangeMessages, int newViewNum){
		List<PrepareMessage> prepareList = viewChangeMessages.stream()
				.flatMap(v -> v.getMessageList().stream())
				.flatMap(pm -> pm.getPrepareMessages().stream())
				.sorted(Comparator.comparingInt(PrepareMessage::getSeqNum))
				.collect(Collectors.toList());
		if (prepareList.size() == 0) {
			List<PreprepareMessage> nullPrepre = null;
			return nullPrepre;
		}

		int min_s = prepareList.stream()
				.min(Comparator.comparingInt(PrepareMessage::getSeqNum))
				.get()
				.getSeqNum();

		int max_s = prepareList.stream()
				.max(Comparator.comparingInt(PrepareMessage::getSeqNum))
				.get()
				.getSeqNum();

		/*
			nullPre_preparesStream: Stream of Pre-prepare Msg with Diget = null (second case of paper)
		 */
		Stream<PreprepareMessage> nullPre_preparesStream = IntStream.rangeClosed(min_s, max_s)
				.filter(n -> prepareList.stream().noneMatch(p -> p.getSeqNum() == n))
				.sorted()
				.mapToObj(n -> makePrePrepareMsg(replica.getPrivateKey(), newViewNum, n, null))
				.distinct();
		/*
			received_pre_prepares: Stream of Pre-prepare Msg that was in set of viewChangeMessages
		 */
		List<PreprepareMessage> received_pre_prepares = viewChangeMessages.stream()
				.map(v -> v.getMessageList())
				.flatMap(pm -> pm.stream())
				.map(pm -> pm.getPreprepareMessage())
				.distinct()
				.collect(Collectors.toList());

		Function<PrepareMessage, RequestMessage> getOp = p -> received_pre_prepares.stream()
				.filter(pp -> pp.equals(p))
				.findAny()
				.get()
				.getRequestMessage();
		/**
			make Stream of Pre-prepare Msgs that has non-null Operation Op.
			Please note that "Op" is not Digest of operation, Not like original paper. cause of Development Convenience
		 */
		Stream<PreprepareMessage> pre_preparesStream = prepareList.stream()
				.map(p -> makePrePrepareMsg(replica.getPrivateKey(), newViewNum, p.getSeqNum(), getOp.apply(p)))
				.distinct();
		/*
			make Sorted Set that has null-Pre-pre & not-null-Pre-pre
		 */

		List<PreprepareMessage> pre_prepares = Stream.concat(nullPre_preparesStream, pre_preparesStream)
				.sorted(Comparator.comparingInt(PreprepareMessage::getSeqNum))
				.collect(Collectors.toList());

		return pre_prepares;
	}

	private static List<ViewChangeMessage> getViewChangeMessages(Function<String, PreparedStatement> queryFn, int newViewNum) throws SQLException {
		/* Replica.canMakeNewViewMessage에서 replica 자신의 view-change 메시지가 있는지, 2f개의 다른 backup들의 메시지가 있는지 이미 검증한다. */

		String query = "SELECT V.data FROM ViewChanges V " +
				"WHERE V.newViewNum = ? " +
				"AND V.checkpointNum = (SELECT MAX(V3.checkpointNum) FROM ViewChanges V3)";    /* newViewNum은 단조증가함! */
		try (var pstmt = queryFn.apply(query)) {
			pstmt.setInt(1, newViewNum);
			var ret = pstmt.executeQuery();
			List<ViewChangeMessage> viewChangeMessages = JdbcUtils.toStream(ret)
					.map(rethrow().wrapFunction(rs -> Util.desToObject(rs.getString(1), ViewChangeMessage.class)))
					.collect(Collectors.toList());

			return viewChangeMessages;
		}
	}

	public boolean isVerified(Replica replica) {
		Map<SocketChannel, PublicKey> keymap = replica.getPublicKeyMap();
		int newPrimaryNum = getNewViewNum() % replica.getReplicaMap().size();

		Boolean[] checklist = new Boolean[4];

		checklist[0] = this.verify(keymap.get(replica.getReplicaMap().get(newPrimaryNum)));

		checklist[1] = this.getViewChangeMessageList().stream()
				.allMatch(v -> v.verify(keymap.get(replica.getReplicaMap().get(v.getReplicaNum()))));

		List<PrepareMessage> agreed_prepares = this.getViewChangeMessageList()
				.stream()
				.flatMap(v -> v.getMessageList().stream())
				.flatMap(pm -> pm.getPrepareMessages().stream())
				.collect(Collectors.toList());
		if (this.getOperationList() == null) {
			checklist[2] = checklist[3] = true;
		} else {
			checklist[2] = this.getOperationList()
					.stream()
					.filter(pp -> pp.getDigest() != null)
					.filter(pp -> agreed_prepares.stream()
							.filter(p -> p.equals(pp))
							.filter(p -> p.verify(keymap.get(replica.getReplicaMap().get(p.getReplicaNum()))))
							.count() > 2 * replica.getMaximumFaulty()
					)
					.count() == this.getOperationList().stream().filter(pp -> pp.getDigest() != null).count();

			checklist[3] = this.getOperationList()
					.stream()
					.filter(pp -> pp.getDigest() == null)
					.noneMatch(pp -> agreed_prepares.stream().anyMatch(p -> p.equals(pp)));
		}

		return Arrays.stream(checklist).allMatch(Boolean::booleanValue);
	}

	public int getNewViewNum() {
		return data.newViewNum;
	}

	public boolean verify(PublicKey publicKey) {
		return Util.verify(publicKey, this.data, this.signature);
	}

	public List<ViewChangeMessage> getViewChangeMessageList() {
		return data.viewChangeMessageList;
	}

	public List<PreprepareMessage> getOperationList() {
		return data.operationList;
	}

	private static class Data implements Serializable {
		private final int newViewNum;
		private final List<ViewChangeMessage> viewChangeMessageList;
		private final List<PreprepareMessage> operationList;

		private Data(int newViewNum, List<ViewChangeMessage> viewChangeMessageList, List<PreprepareMessage> operationList) {
			this.newViewNum = newViewNum;
			this.viewChangeMessageList = viewChangeMessageList;
			this.operationList = operationList;
		}
	}
}
