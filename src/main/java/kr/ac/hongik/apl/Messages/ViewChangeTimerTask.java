package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Replica;

import static com.diffplug.common.base.Errors.rethrow;

public class ViewChangeTimerTask extends java.util.TimerTask {
	final int checkpointNum;
	final int newViewNum;
	final Replica replica;


	public ViewChangeTimerTask(int checkpointNum, int newViewNum, Replica replica) {
		this.replica = replica;
		this.checkpointNum = checkpointNum;
		this.newViewNum = newViewNum;

	}

	@Override
	public void run() {
		replica.setViewChangePhase(true);
		if (Replica.DEBUG) {
			System.err.print("Enter ViewChange Phase ");
		}
		//Cancel and remove newViewNum'th timer

		replica.removeNewViewTimer(newViewNum);
		replica.removeViewChangeTimer();

		var keySet = replica.getTimerMap().keySet();
		keySet.removeAll(keySet);
		var getPreparedStatementFn = rethrow().wrap(replica.getLogger()::getPreparedStatement);
		try {
			if (Replica.DEBUG) {
				System.out.println("	Checkpoint num " + replica.getWatermarks()[0] + " newViewNum " + newViewNum);
			}
			synchronized (replica.watermarkLock) {
				ViewChangeMessage viewChangeMessage = ViewChangeMessage.makeViewChangeMsg(replica.getWatermarks()[0], newViewNum, replica, getPreparedStatementFn);
				Replica.getReplicaMap().values().forEach(sock -> replica.send(sock, viewChangeMessage));
			}
		} finally {

		}
	}


}
