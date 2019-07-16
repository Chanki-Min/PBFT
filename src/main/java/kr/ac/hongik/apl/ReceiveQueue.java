package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.Message;

import java.util.concurrent.PriorityBlockingQueue;

public class ReceiveQueue extends Thread {
	Replica replica;

	public ReceiveQueue(Replica replica) {
		this.replica = replica;
	}

	public void run(PriorityBlockingQueue<Message> socketQueue) {
		while (true) {
			socketQueue.offer(this.replica.offerQueue());
		}
	}
}
