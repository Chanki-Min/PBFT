package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Replica;
import kr.ac.hongik.apl.Util;
import org.echocat.jsu.JdbcUtils;

import java.io.Serializable;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.diffplug.common.base.Errors.rethrow;
import static kr.ac.hongik.apl.Messages.PreprepareMessage.makePrePrepareMsg;

public class NewViewMessage implements Message {
	private final Data data;
	private final byte[] signature;

	private NewViewMessage(Data data, byte[] signature) {
		this.data = data;
		this.signature = signature;
	}

	public static NewViewMessage makeNewViewMessage(Replica replica, int newViewNum) throws SQLException {
		Function<String, PreparedStatement> queryFn = rethrow().wrap(replica.getLogger()::getPreparedStatement);
		List<ViewChangeMessage> viewChangeMessages = getViewChangeMessages(queryFn);    //GC가 이미 끝나서 DB안에는 last checkpoint 이후만 있다고 가정
		List<PreprepareMessage> operationList = getOperationList(replica, newViewNum);
		Data data = new Data(newViewNum, viewChangeMessages, operationList);
		byte[] signature = Util.sign(replica.getPrivateKey(), data);


		return new NewViewMessage(data, signature);
	}

	public boolean isVerified(PublicKey key) {
		Boolean[] checklist = new Boolean[3];

		checklist[0] = this.verify(key);

		var agreed_prepares = this.getViewChangeMessageList()
				.stream()
				.flatMap(v -> v.getMessageList().stream())
				.flatMap(pm -> pm.getPrepareMessages().stream());

		checklist[1] = this.getOperationList()
				.stream()
				.filter(pp -> pp.getDigest() != null)
				.allMatch(pp -> agreed_prepares.anyMatch(p -> p.getDigest().equals(pp.getDigest())));

		checklist[2] = this.getOperationList()
				.stream()
				.filter(pp -> pp.getDigest() == null)
				.noneMatch(pp -> agreed_prepares.anyMatch(p -> p.getDigest().equals(pp.getDigest())));

		return Arrays.stream(checklist).allMatch(x -> x);
	}

	private static List<ViewChangeMessage> getViewChangeMessages(Function<String, PreparedStatement> queryFn) throws SQLException {
		/* Replica.canMakeNewViewMessage에서 replica 자신의 view-change 메시지가 있는지, 2f개의 다른 backup들의 메시지가 있는지 이미 검증한다. */

		String query = "SELECT V1.data FROM ViewChanges V1 " +
				"WHERE AND V1.newViewNum = (SELECT MAX(V2.newViewNum) FROM ViewChanges V2 ) ";	/* newViewNum은 단조증가함! */
		try (var pstmt = queryFn.apply(query)) {
			var ret = pstmt.executeQuery();
			List<ViewChangeMessage> viewChangeMessages = JdbcUtils.toStream(ret)
					.map(rethrow().wrapFunction(rs -> Util.desToObject(rs.getString(1), ViewChangeMessage.class)))
					.collect(Collectors.toList());

			return viewChangeMessages;
		}
	}

	private static List<PreprepareMessage> getOperationList(Replica replica, int newViewNum) throws SQLException {
		String query = "SELECT V.data FROM ViewChanges V " +
				"WHERE V.newViewNum = ? " +
				"AND V.checkpointNum = (SELECT MAX(V3.checkpointNum) FROM ViewChanges V3)";
		try (var pstmt = replica.getLogger().getPreparedStatement(query)) {
			pstmt.setInt(1, newViewNum);
			ResultSet rs = pstmt.executeQuery();

			List<ViewChangeMessage> viewChangeMessages = JdbcUtils.toStream(rs)
					.map(rethrow().wrapFunction(x -> x.getString(1)))
					.map(x -> Util.desToObject(x, ViewChangeMessage.class))
					.collect(Collectors.toList());

			List<PrepareMessage> prepareList = viewChangeMessages.stream()
					.flatMap(v -> v.getMessageList().stream())
					.flatMap(pm -> pm.getPrepareMessages().stream())
					.sorted(Comparator.comparingInt(PrepareMessage::getSeqNum))
					.collect(Collectors.toList());


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
					.mapToObj(n -> makePrePrepareMsg(replica.getPrivateKey(), newViewNum, n, null));
			/*
				received_pre_prepares: Stream of Pre-prepare Msg that was in set of viewChangeMessages
			 */
			Stream<PreprepareMessage> received_pre_prepares = viewChangeMessages.stream()
					.map(v -> v.getMessageList())
					.flatMap(pm -> pm.stream())
					.map(pm -> pm.getPreprepareMessage())
					.distinct();

			Function<PrepareMessage, Operation> getOp = p -> received_pre_prepares
					.filter(pp -> pp.getDigest().equals(p.getDigest()))
					.findAny()
					.get()
					.getOperation();
			/**
				make Stream of Pre-prepare Msgs that has non-null Operation Op.
			    Please note that "Op" is not Digest of operation, Not like original paper. cause of Development Convenience
			 */
			Stream<PreprepareMessage> pre_preparesStream = prepareList.stream()
					.map(p -> makePrePrepareMsg(replica.getPrivateKey(), newViewNum, p.getSeqNum(), getOp.apply(p)));
			/*
				make Sorted Set that has null-Pre-pre & not-null-Pre-pre
			 */
			List<PreprepareMessage> pre_prepares = Stream.concat(nullPre_preparesStream, pre_preparesStream)
					.sorted(Comparator.comparingInt(PreprepareMessage::getSeqNum))
					.collect(Collectors.toList());


			return pre_prepares;
		}

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
