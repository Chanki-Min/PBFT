package kr.ac.hongik.apl;

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
		var getPreparedStatementFn = rethrow().wrap(replica.getLogger()::getPreparedStatement);
		ViewChangeMessage viewChangeMessage = ViewChangeMessage.makeViewChangeMsg(replica.getPrivateKey(), checkpointNum, newViewNum, replica.getMyNumber(), getPreparedStatementFn);

		replica.replicas.values().forEach(sock -> replica.send(sock, viewChangeMessage));
	}
}
