package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Replica;

import static com.diffplug.common.base.Errors.rethrow;

public class ViewChangeTimerTask extends java.util.TimerTask {
	final int checkpointNum;
	final int newViewNum;
	final int currentViewNum;
	final Replica replica;


	public ViewChangeTimerTask(int checkpointNum, int newViewNum, Replica replica) {
		this.replica = replica;
		this.checkpointNum = checkpointNum;
		this.newViewNum = newViewNum;
		this.currentViewNum = replica.getViewNum();
	}

	@Override
	public void run() {
		synchronized (replica.viewChangeLock) {
			if(replica.getViewNum() >= newViewNum){
				return;
			}
			replica.setViewChangePhase(true);
		}
		Replica.msgDebugger.info(String.format("Enter ViewChange Phase, gap: %d", this.newViewNum - this.currentViewNum));
		//Cancel and remove newViewNum'th timer

		replica.removeNewViewTimer(newViewNum);
		replica.removeViewChangeTimer();

		var getPreparedStatementFn = rethrow().wrap(replica.getLogger()::getPreparedStatement);
		try {
			Replica.detailDebugger.trace(String.format("Checkpoint num : %d NewViewNum : %d", replica.getWatermarks()[0], newViewNum));
			synchronized (replica.watermarkLock) {
				ViewChangeMessage viewChangeMessage = ViewChangeMessage.makeViewChangeMsg(replica.getWatermarks()[0], newViewNum, replica, getPreparedStatementFn);
				Replica.getReplicaMap().values().forEach(sock -> replica.send(sock, viewChangeMessage));
			}
		} finally {

		}
	}


}
