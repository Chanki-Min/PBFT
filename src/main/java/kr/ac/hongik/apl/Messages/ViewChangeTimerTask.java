package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Replica;

import static com.diffplug.common.base.Errors.rethrow;

public class ViewChangeTimerTask extends java.util.TimerTask {
	final int checkpointNum;
	final int newViewNum;
	final Replica replica;
	boolean DEBUG = true;

	public ViewChangeTimerTask(int checkpointNum, int newViewNum, Replica replica) {
		this.replica = replica;
		this.checkpointNum = checkpointNum;
		this.newViewNum = newViewNum;
	}

	/*
	TODO:
	 비동기로 터지다보니 GC 하는 중에 터져서 다른 레플리카들이랑 stable checkpoint가 다름(Checkpoint message list size = 0)
	 이 경우 처리해줘야 함
	 */
	@Override
	public void run() {
		replica.setViewChangePhase(true);
		if (DEBUG) {
			System.err.println("Enter ViewChange Phase");
		}
		//Cancel and remove newViewNum'th timer

		replica.removeNewViewTimer(newViewNum);
		replica.removeViewChangeTimer();

		var keySet = replica.getTimerMap().keySet();
		keySet.removeAll(keySet);
		var getPreparedStatementFn = rethrow().wrap(replica.getLogger()::getPreparedStatement);
		try {
			if (DEBUG) {
				System.out.println("Checkpoint num " + checkpointNum + " newViewNum " + newViewNum);
			}
			ViewChangeMessage viewChangeMessage = ViewChangeMessage.makeViewChangeMsg(checkpointNum, newViewNum, replica, getPreparedStatementFn);
			replica.getReplicaMap().values().forEach(sock -> replica.send(sock, viewChangeMessage));
		} finally {

		}
	}


}
