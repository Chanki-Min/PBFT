package kr.ac.hongik.apl;

import org.apache.commons.lang3.NotImplementedException;
import org.echocat.jsu.JdbcUtils;

import java.io.Serializable;
import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.diffplug.common.base.Errors.rethrow;

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
		List<PreprepareMessage> operationList = getOperationList(queryFn);
		Data data = new Data(newViewNum, viewChangeMessages, operationList);
		byte[] signature = Util.sign(replica.getPrivateKey(), data);


		return new NewViewMessage(data, signature);
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

	private static List<PreprepareMessage> getOperationList(Function<String, PreparedStatement> queryFn) {
		//TODO: getOperationList
		throw new NotImplementedException("필요한 쿼리:" +
				"View-change message DB를 검색하며 가장 높은 sequence number 및 가장 낮은 sequence number를 구한다." +
				"각 sequence number n에 대해 새로 new view number에 기반한 pre-prepare message를 생성한다.");
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
