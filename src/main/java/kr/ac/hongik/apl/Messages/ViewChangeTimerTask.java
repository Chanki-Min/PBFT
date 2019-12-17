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
		synchronized (replica.viewChangeLock) {
			if(replica.getViewNum() >= newViewNum){
				return;
			}
			replica.setViewChangePhase(true);
		}
		Replica.msgDebugger.info(String.format("Enter ViewChange Phase"));
		//Cancel and remove newViewNum'th timer

		replica.removeNewViewTimer(newViewNum);
		replica.removeViewChangeTimer();

		//TODO : this.newViewNum을 key로 가지지 않느느 NewViewTimer는 살려야 함 (지금은 전부 지워버림), 어차피 위에 2함수에서 자기꺼 지우기까지 해주니까 아래 2줄은 없어도 됨
		var keySet = replica.getTimerMap().keySet();
		keySet.removeAll(keySet);
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
