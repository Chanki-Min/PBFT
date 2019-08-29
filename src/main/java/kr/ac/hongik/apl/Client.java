package kr.ac.hongik.apl;


import kr.ac.hongik.apl.Messages.HeaderMessage;
import kr.ac.hongik.apl.Messages.Message;
import kr.ac.hongik.apl.Messages.ReplyMessage;
import kr.ac.hongik.apl.Messages.RequestMessage;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.PublicKey;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Client extends Connector {
	private HashMap<Long, Integer[]> replies = new HashMap<>();
	private HashMap<Long, List<Object>> distributedReplies = new HashMap<>();
	private List<Long> ignoreList = new ArrayList<>();
	private Map<Long, Timer> timerMap = new ConcurrentHashMap<>();    /* Key: timestamp, value: timer  */


	public Client(Properties prop) {
		super(prop);    //make socket to every replica
		super.connect();
	}

	public static void main(String[] args) {
	}

	@Override
	protected void sendHeaderMessage(SocketChannel channel) {
		HeaderMessage headerMessage = new HeaderMessage(-1, this.getPublicKey(), "client");
		send(channel, headerMessage);
		if (Replica.DEBUG) {
			System.err.println("send headerMessage, key: " + headerMessage.getPublicKey().toString().substring(46, 66));
		}
	}

	//Empty method.
	@Override
	protected void acceptOp(SelectionKey key) {
		throw new UnsupportedOperationException("Client class does not need this method.");
	}

	public void request(RequestMessage msg) {
		BroadcastTask task = new BroadcastTask(msg, this, this.timerMap, 1);
		Timer timer = new Timer();
		if (Replica.DEBUG)
			timer.schedule(task, TIMEOUT * 2);
		else
			timer.schedule(task, TIMEOUT);
		timerMap.put(msg.getTime(), timer);

		int idx = new Random().nextInt(getReplicaMap().size());
		if (Replica.DEBUG) {
			System.err.println("send to = " + idx);
		}
		var sock = getReplicaMap().values().stream().skip(idx).findFirst().get();

		send(sock, msg);
	}

	public Object getReply() {
		ReplyMessage replyMessage;


		while (true) {
			Message message = null;
			try {
				message = receive();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (message instanceof HeaderMessage) {
				handleHeaderMessage((HeaderMessage) message);
				continue;
			}
			replyMessage = (ReplyMessage) message;
			// check client info
			PublicKey publicKey = this.publicKeyMap.get(getReplicaMap().get(replyMessage.getReplicaNum()));
			if (replyMessage.verifySignature(publicKey)) {
				Long uniqueKey = replyMessage.getTime();
				Integer[] checkReplica;

				/*
				 * 한 클라이언트가 여러 request를 보낼 시, 그 request들을 구분해주는 것은 timestamp이므로,
				 * Timestamp값을 이용하여 여러 요청들을 구분한다.
				 */

				if (replies.containsKey(uniqueKey)) {
					checkReplica = replies.get(uniqueKey);
				} else {
					checkReplica = new Integer[getReplicaMap().size()];
					if(Replica.DEBUG)
						System.err.println(checkReplica.length);
					for(int i=0; i<getReplicaMap().size();i++)
						checkReplica[i] = 0;
				}

				checkReplica[replyMessage.getReplicaNum()] = 1;
				replies.put(uniqueKey, checkReplica);
				checkReplica = replies.get(uniqueKey);

				if (Arrays.stream(checkReplica).filter(x -> x.intValue() == 1).count() > 2 * this.getMaximumFaulty()) {
					if (!ignoreList.contains(uniqueKey)) {
						ignoreList.add(uniqueKey);

						//Release timer
						if (Replica.DEBUG) {
							System.err.println("Got reply : " + replyMessage.getTime());
							System.err.print("Before Timer cancel\n\t");
							timerMap.keySet().stream().forEach(x -> System.err.print(x + " "));
						}
						timerMap.get(replyMessage.getTime()).cancel();
						timerMap.remove(replyMessage.getTime());
						if (Replica.DEBUG) {
							System.err.print("\nAfter Timer cancel\n\t");
							timerMap.keySet().stream().forEach(x -> System.err.print(x + " "));
							System.err.println(" ");
						}
						return replyMessage.getResult();
					}
				}
			} else {
				System.err.println("Unverified message");
			}
		}
	}

	private void handleHeaderMessage(HeaderMessage message) {
		HeaderMessage header = message;
		if (!header.getType().equals("replica")) throw new AssertionError();
		SocketChannel channel = header.getChannel();
		this.publicKeyMap.put(channel, header.getPublicKey());
		getReplicaMap().put(header.getReplicaNum(), channel);
	}


	static class BroadcastTask extends TimerTask {

		final private Connector conn;
		final private int timerCount;
		RequestMessage requestMessage;
		private Map<Long, Timer> timerMap;

		BroadcastTask(RequestMessage requestMessage, Connector conn, Map<Long, Timer> timerMap, int timerCount) {
			this.requestMessage = requestMessage;
			this.conn = conn;
			this.timerMap = timerMap;
			this.timerCount = timerCount;
		}

		@Override
		public void run() {
			if (Replica.DEBUG) {
				System.err.println((timerCount) + " Timer expired! timestamp : " + requestMessage.getTime());
			}
			RequestMessage nextRequestMessage = requestMessage;
			if (timerCount > 1) {
				nextRequestMessage = RequestMessage.makeRequestMsg(conn.getPrivateKey(), requestMessage.getOperation().update());
			}
			BroadcastTask task = new BroadcastTask(nextRequestMessage, this.conn, this.timerMap, this.timerCount + 1);
			RequestMessage finalNextRequestMessage = nextRequestMessage;
			getReplicaMap().values().forEach(socket -> conn.send(socket, finalNextRequestMessage));
			Timer timer = new Timer();
			timer.schedule(task, (this.timerCount + 1) * TIMEOUT);

			timerMap.put(nextRequestMessage.getTime(), timer);
		}
	}
}
