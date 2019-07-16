package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.HeaderMessage;
import kr.ac.hongik.apl.Messages.Message;
import kr.ac.hongik.apl.Messages.ReplyMessage;
import kr.ac.hongik.apl.Messages.RequestMessage;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.PublicKey;
import java.util.*;

public class Client extends Connector {
	private HashMap<Long, Integer> replies = new HashMap<>();
	private HashMap<Long, List<Object>> distributedReplies = new HashMap<>();
	private List<Long> ignoreList = new ArrayList<>();
	private final Boolean DEBUG = true;

	private Map<Long, Timer> timerMap = new HashMap<>();    /* Key: timestamp, value: timer  */


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
		if(DEBUG) {
			System.err.println("send headerMessage, key: " + headerMessage.getPublicKey().toString().substring(46, 66));
		}
	}

	//Empty method.
	@Override
	protected void acceptOp(SelectionKey key) {
		throw new UnsupportedOperationException("Client class does not need this method.");
	}

	public void request(RequestMessage msg) {
		BroadcastTask task = new BroadcastTask(msg, this);
		Timer timer = new Timer();
		timer.schedule(task, TIMEOUT);
		timerMap.put(msg.getTime(), timer);
		this.getReplicaMap().values().forEach(channel -> this.send(channel, msg));
		int idx = new Random().nextInt(getReplicaMap().size());
		var sock = getReplicaMap().values().stream().skip(idx).findFirst().get();
		send(sock, msg);
	}

	Object getReply() {
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
			PublicKey publicKey = this.publicKeyMap.get(this.getReplicaMap().get(replyMessage.getReplicaNum()));
			if (replyMessage.verifySignature(publicKey)) {
				long uniqueKey = replyMessage.getTime();
				if (replyMessage.isDistributed()) {
					/******    각 레플리카가 각기 다른 메시지를 전달할 경우, 2f + 1개의 메시지를 수집한다.    ******/
					List<Object> replyMsgs = distributedReplies.getOrDefault(uniqueKey, new ArrayList<>());
					distributedReplies.put(uniqueKey, replyMsgs);

					replyMsgs.add(replyMessage.getResult());

					if (replyMsgs.size() > 2 * getMaximumFaulty()) {
						if (!ignoreList.contains(uniqueKey)) {
							ignoreList.add(uniqueKey);
							return replyMsgs;
						}
					}
				} else {
					/*
					 * 한 클라이언트가 여러 request를 보낼 시, 그 request들을 구분해주는 것은 timestamp이므로,
					 * Timestamp값을 이용하여 여러 요청들을 구분한다.
					 */
					Integer numOfValue = replies.getOrDefault(uniqueKey, 0);
					replies.put(uniqueKey, numOfValue + 1);
					if (replies.get(uniqueKey) > this.getMaximumFaulty()) {
						if (!ignoreList.contains(uniqueKey)) {
							ignoreList.add(uniqueKey);

							//Release timer
							timerMap.get(replyMessage.getTime()).cancel();
							timerMap.remove(replyMessage.getTime());

							return replyMessage.getResult();
						}
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
		this.getReplicaMap().put(header.getReplicaNum(), channel);
	}


	static class BroadcastTask extends TimerTask {

		final RequestMessage requestMessage;
		final private Connector conn;

		BroadcastTask(RequestMessage requestMessage, Connector conn) {
			this.requestMessage = requestMessage;
			this.conn = conn;
		}

		@Override
		public void run() {
			conn.getReplicaMap().values().forEach(socket -> conn.send(socket, requestMessage));
		}
	}
}
