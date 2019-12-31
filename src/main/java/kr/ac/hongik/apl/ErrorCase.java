package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.*;
import kr.ac.hongik.apl.Operations.GreetingOperation;
import kr.ac.hongik.apl.Operations.Operation;
import kr.ac.hongik.apl.Operations.OperationExecutionException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SocketChannel;
import java.util.Properties;

import static java.lang.Thread.sleep;
import static kr.ac.hongik.apl.Messages.PrepareMessage.makePrepareMsg;
import static kr.ac.hongik.apl.Messages.PreprepareMessage.makePrePrepareMsg;
import static kr.ac.hongik.apl.Messages.ReplyMessage.makeReplyMsg;

/**
 * ViewChange 단계 테스팅을 위하여 모든 장애 경우를 테스트하는 클래스
 */
public class ErrorCase {
	public static void doFaulty(Replica replica, int errno, int primaryErrSeqNum, PreprepareMessage preprepareMessage) throws OperationExecutionException {
		int seqNum = preprepareMessage.getSeqNum();
		if ((seqNum % 10 == primaryErrSeqNum && 0 == replica.getViewNum() % Connector.getReplicaMap().size()) ||
				(seqNum % 10 == primaryErrSeqNum - 1 && 1 == replica.getViewNum() % Connector.getReplicaMap().size()) ||
				(seqNum % 10 == primaryErrSeqNum - 2 && 2 == replica.getViewNum() % Connector.getReplicaMap().size()) ||
				(seqNum % 10 == primaryErrSeqNum - 3 && 3 == replica.getViewNum() % Connector.getReplicaMap().size())
		) {
			if (errno == 1) { //primary stops suddenly[
				System.err.println("Faulty Case I'm gonna sleep at #" + seqNum);
				primaryStopCase(replica);
			} else if (errno == 2) { //primary sends bad pre-prepare message which consists of wrong request message
				System.err.println("Faulty Case I'm sending Bad prepre at #" + seqNum);
				primarySendBadPrepreCase(replica, seqNum);
			} else if (errno == 3) { // primary sends reply messages more than 2*f and does not broadcast pre-prepare message
				System.err.println("Faulty Case I'm sending all replys at #" + seqNum);
				primarySendAllReplyMsg(replica, seqNum, preprepareMessage);
			} else if (errno == 4) { // primary sends reply messages more than 2*f and broadcast pre-prepare message
				System.err.println("Faulty Case I'm sending all replys and valid prepre at #" + seqNum);
				primarySendAllReplyMsg(replica, seqNum, preprepareMessage);
				Connector.getReplicaMap().values().forEach(channel -> replica.send(channel, preprepareMessage));
			} else {
				Connector.getReplicaMap().values().forEach(channel -> replica.send(channel, preprepareMessage));
			}
			return;
		} else {
			Connector.getReplicaMap().values().forEach(channel -> replica.send(channel, preprepareMessage));
		}
	}

	public static void primaryStopCase(Replica replica) {
		//exit(1);
		try {
			sleep(Replica.TIMEOUT * 3);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void primarySendBadPrepreCase(Replica replica, int seqNum) {
		InputStream in = replica.getClass().getResourceAsStream("/replica.properties");
		Properties prop = new Properties();
		try {
			prop.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}

		Client client = new Client(prop);
		Operation op = new GreetingOperation(client.getPublicKey());
		RequestMessage requestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), op);
		PreprepareMessage preprepareMessage = makePrePrepareMsg(replica.getPrivateKey(), replica.getViewNum(), seqNum, requestMessage);
		Connector.getReplicaMap().values().forEach(channel -> replica.send(channel, preprepareMessage));
	}

	public static void primarySendAllReplyMsg(Replica replica, int seqNum, PreprepareMessage preprepareMessage) throws OperationExecutionException {
		PrepareMessage prepareMessage = makePrepareMsg(replica.getPrivateKey(), replica.getViewNum(), seqNum, preprepareMessage.getDigest(), replica.getMyNumber());
		CommitMessage commitMessage = CommitMessage.makeCommitMsg(replica.getPrivateKey(), replica.getViewNum(), seqNum, prepareMessage.getDigest(), replica.getMyNumber());
		var operation = preprepareMessage.getOperation();
		Object ret = operation.execute(replica.getLogger());

		var viewNum = replica.getViewNum();
		var timestamp = operation.getTimestamp();
		var clientInfo = operation.getClientInfo();
		ReplyMessage replyMessage = makeReplyMsg(replica.getPrivateKey(), viewNum, timestamp,
				clientInfo, replica.getMyNumber(), ret);

		replica.getLogger().insertMessage(prepareMessage);
		replica.getLogger().insertMessage(commitMessage);
		replica.getLogger().insertMessage(seqNum, replyMessage);

		SocketChannel destination = replica.getChannelFromClientInfo(replyMessage.getClientInfo());
		for (int i = 0; i < 4; i++)
			replica.send(destination, replyMessage);
		return;

	}
}
