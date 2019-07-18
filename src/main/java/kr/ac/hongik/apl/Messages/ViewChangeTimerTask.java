package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Replica;

import java.util.List;
import java.util.stream.Collectors;

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


	@Override
	public void run() {
		replica.setViewChangePhase(true);
		if (DEBUG) {
			System.err.println("Enter ViewChange Phase");
		}
		//Cancel and remove newViewNum'th timer
		List<String> deletableKeys = replica.getTimerMap()
				.entrySet()
				.stream()
				.filter(x -> x.getKey().equals("view: " + newViewNum))
				.peek(x -> x.getValue().cancel())
				.map(x -> x.getKey())
				.collect(Collectors.toList());

		replica.getTimerMap().entrySet().removeAll(deletableKeys);

		//Cancel and remove view change timers made by backups
		deletableKeys = replica.getTimerMap()
				.entrySet()
				.stream()
				.filter(x -> !x.getKey().startsWith("view: "))
				.peek(x -> x.getValue().cancel())
				.map(x -> x.getKey())
				.collect(Collectors.toList());

		replica.getTimerMap().entrySet().removeAll(deletableKeys);



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
