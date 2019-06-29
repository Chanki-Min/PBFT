package kr.ac.hongik.apl;

import java.util.Timer;

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
		replica.getTimerMap().values().stream().forEach(timer -> timer.cancel());
		var keySet = replica.getTimerMap().keySet();
		keySet.removeAll(keySet);
		var getPreparedStatementFn = rethrow().wrap(replica.getLogger()::getPreparedStatement);
		ViewChangeMessage viewChangeMessage = ViewChangeMessage.makeViewChangeMsg(replica.getPrivateKey(), checkpointNum, newViewNum, replica.getMyNumber(), getPreparedStatementFn);

		replica.replicas.values().forEach(sock -> replica.send(sock, viewChangeMessage));

		ViewChangeTimerTask viewChangeTimerTask = new ViewChangeTimerTask(checkpointNum, newViewNum + 1, replica);
		Timer timer = new Timer();
		timer.schedule(viewChangeTimerTask,
				replica.getTimerMap().put("View: " + (newViewNum + 1), )

				/* TODO: new view v + i 에 대해서 Timeout * i 만큼 기다린다. 이 기간 내에 new-view message를 받지 못한다면 i := i + 1로 새로운 view-change message를 생성한다. */
	}
}
