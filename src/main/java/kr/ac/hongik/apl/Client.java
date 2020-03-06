package kr.ac.hongik.apl;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import kr.ac.hongik.apl.Messages.*;
import kr.ac.hongik.apl.Operations.OperationExecutionException;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.security.PublicKey;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Client extends Connector implements Closeable {

	private final boolean MEASURE;
	private HashMap<Long, HashSet<Integer>> replies = new HashMap<>();
	private List<Long> ignoreList = new ArrayList<>();
	private Map<Long, Timer> timerMap = new ConcurrentHashMap<>();    /* Key: timestamp, value: timer  */
	private Long receivingTimeStamp;
	private final Object replyLock = new Object();
	private HashMap<Long, Long> turnAroundTimeMap = new HashMap<>();

	public Client(Properties prop) {
		super(prop);    //make socket to every replica
		super.connect();
		this.MEASURE = Boolean.parseBoolean(prop.getProperty(PropertyNames.REPLICA_MEASURE));
	}

	@Override
	protected void sendHeaderMessage(SocketChannel channel) {
		HeaderMessage headerMessage = new HeaderMessage(-1, this.getPublicKey(), "client");
		send(channel, headerMessage);

		Replica.msgDebugger.debug(String.format("Send Header msg, key : %s", headerMessage.getPublicKey().toString().substring(46,66)));
	}

	/*
	 * 로직
	 * 1.Client가 closeConnectionMsg send
	 * 2.Replica가 수신
	 * 3.Replica.super.publicKeyMap 에서 Client의 소켓 채널 삭제
	 * 4.Replica.super에서 client 소켓 채널 disconnect
	 * 5.Client.super.publicKeyMap에서 replica의 소켓 채널 삭제
	 * 6.Client.super에서 replica의 소켓 채널 disconnect
	 * 7. replica 의 Excuted에서 clientInfo인 레코드 삭제
	 */
	public void close() {
		//disconnect from every replica
		CloseConnectionMessage closeConnectionMessage = new CloseConnectionMessage(-1, this.getPublicKey(), "client");
		ignoreList.clear();
		replies.clear();
		//cancle all timer and clear timerMap
		timerMap.values().forEach(Timer::cancel);
		timerMap.clear();
		var itr = publicKeyMap.keySet().iterator();
		while(itr.hasNext()) {
			SocketChannel channel = itr.next();
			send(channel, closeConnectionMessage);
			itr.remove();
			try {
				channel.close();
			} catch (IOException e) {
				Replica.msgDebugger.error(e);
			}
		}
	}

	/**
	 * requestMessage를 임의의 replica에게 보내고 만약 요청이 오지 않을 경우를 대비하여 broadcastTimer를 설정한다.
	 * 타이머 만료시 모든 replica들에게 requestMsg를 broadcast하여 viewChange Phase를 유도한다
	 *
	 * @param msg 요청할 requestMessage
	 */
	public void request(RequestMessage msg) {
		synchronized (replyLock) {
			BroadcastTask task = new BroadcastTask(msg, this, this.timerMap, 1);
			Timer timer = new Timer();
			timer.schedule(task, viewChangeTimeOutMillis);
			timerMap.put(msg.getTime(), timer);
			if(this.MEASURE){
				getReplicaMap().values().stream().forEach(x -> send(x, msg));
				setReceivingTimeStamp(msg.getTime());
				return;
			}
			int idx = new Random().nextInt(getReplicaMap().size());

			Replica.msgDebugger.debug(String.format("Send Request Msg to %d", idx));
			checkSocketChannel();
			var sock = getReplicaMap().values().stream().skip(idx).findFirst().get();
			send(sock, msg);
			setReceivingTimeStamp(msg.getTime());
		}
	}

	/**
	 * replicas map에 등록된 모든 레플리카에 대해서 소켓을 테스트한다. send가 실패하면 자연스럽게 reconnet가 이어지므로, 재접속을 기대할 수 있다
	 */
	public void checkSocketChannel() {
		Replica.detailDebugger.trace("Checking Socket connection before send message...");
		for (var entry: getReplicaMap().entrySet()) {
			HeartBeatMessage signal = new HeartBeatMessage();
			send(entry.getValue(), signal);
		}
	}

	/**
	 * requestMessage를 임의의 replica에게 보내고 만약 요청이 오지 않을 경우를 대비하여 broadcastTimer를 설정한다.
	 * 타이머 만료시 모든 replica들에게 requestMsg를 broadcast하여 viewChange Phase를 유도한다
	 *
	 * @param msg 요청할 requestMessage
	 * @param replicaNum  요청을 보낼 replica number
	 */
	public void request(RequestMessage msg, int replicaNum) {
		synchronized (replyLock) {
			BroadcastTask task = new BroadcastTask(msg, this, this.timerMap, 1);
			Timer timer = new Timer();
			timer.schedule(task, viewChangeTimeOutMillis);
			timerMap.put(msg.getTime(), timer);
			if(this.MEASURE){
				getReplicaMap().values().stream().forEach(x -> send(x, msg));
				setReceivingTimeStamp(msg.getTime());
				return;
			}
			Replica.msgDebugger.debug(String.format("Send Request Msg to %d", replicaNum));

			var sock = getReplicaMap().values().stream().skip(replicaNum).findFirst().get();

			send(sock, msg);
			setReceivingTimeStamp(msg.getTime());
		}
	}

	/**
	 * Replica 들에게 요청한 request에 대한 reply를 받아온다
	 *
	 * @return 합의된 reply data
	 */
	public Object getReply() {
		ReplyMessage replyMessage;
		while (true) {
			Message message = null;
			try {
				message = receive();
			} catch (InterruptedException e) {
				Replica.msgDebugger.error(e);
				continue;
			}
			if (message instanceof HeaderMessage) {
				handleHeaderMessage((HeaderMessage) message);
				continue;
			}
			replyMessage = (ReplyMessage) message;
			// check is reply object is OperationExecutionException
			if(replyMessage.getResult() instanceof OperationExecutionException) {
				ObjectMapper objectMapper = new ObjectMapper();
				var exception = (OperationExecutionException) replyMessage.getResult();
				Replica.msgDebugger.error("OperationExecutionException occurred ", exception);
				try {
					Replica.msgDebugger.error(objectMapper.writeValueAsString(replyMessage.getData()));
				} catch (JsonProcessingException e) {
					Replica.msgDebugger.error(e);
				}
			}

			// check client info
			PublicKey publicKey = this.publicKeyMap.get(getReplicaMap().get(replyMessage.getReplicaNum()));
			if (replyMessage.verifySignature(publicKey)) {
				Long uniqueKey = replyMessage.getTime();
				HashSet<Integer> checkReplica;

				/*
				 * 한 클라이언트가 여러 request를 보낼 시, 그 request들을 구분해주는 것은 timestamp이므로,
				 * Timestamp값을 이용하여 여러 요청들을 구분한다.
				 */
				if(ignoreList.contains(uniqueKey)) {
					continue;
				} else if (replies.containsKey(uniqueKey)) {
					checkReplica = replies.get(uniqueKey);
				} else {
					checkReplica = new HashSet<>();
				}

				checkReplica.add(replyMessage.getReplicaNum());
				replies.put(uniqueKey, checkReplica);
				synchronized (replyLock) {
					if(replyMessage.getTime() != this.getReceivingTimeStamp()){
						continue;
					}
					if (checkReplica.size() > 2 * this.getMaximumFaulty() && !ignoreList.contains(uniqueKey)) {
						ignoreList.add(uniqueKey);
						replies.remove(uniqueKey);

						//Release broadcast timer
						timerMap.get(replyMessage.getTime()).cancel();
						timerMap.remove(replyMessage.getTime());

						if(this.MEASURE){
							turnAroundTimeMap.put(replyMessage.getTime(), Instant.now().toEpochMilli() - replyMessage.getTime());
							Replica.measureDebugger.info(String.format("Turn Around Time : %f", ((double) (turnAroundTimeMap.get(replyMessage.getTime())) / 1000)));
						}
						Replica.msgDebugger.debug(String.format("Got Reply Msg : %d", replyMessage.getTime()));
						return replyMessage.getResult();
					}
				}
			} else {
				Replica.msgDebugger.error("Unverified Message");
			}
		}
	}

	private void handleHeaderMessage(HeaderMessage message) {
		if (!message.getType().equals("replica")) throw new AssertionError();

		SocketChannel channel = message.getChannel();
		this.publicKeyMap.put(channel, message.getPublicKey());
		getReplicaMap().put(message.getReplicaNum(), channel);
	}

	private void setReceivingTimeStamp(Long timestamp){
		this.receivingTimeStamp = timestamp;
	}

	private Long getReceivingTimeStamp(){
		return receivingTimeStamp;
	}

	static class BroadcastTask extends TimerTask {

		final private Client client;
		final private int timerCount;
		RequestMessage requestMessage;
		private Map<Long, Timer> timerMap;

		BroadcastTask(RequestMessage requestMessage, Client client, Map<Long, Timer> timerMap, int timerCount) {
			this.requestMessage = requestMessage;
			this.client = client;
			this.timerMap = timerMap;
			this.timerCount = timerCount;
		}

		@Override
		public void run() {
			synchronized (client.replyLock) {

				Replica.msgDebugger.debug(String.format("Timer expired! timestamp : ", requestMessage.getTime()));

				RequestMessage nextRequestMessage = requestMessage;
				if (timerCount > 1) {
					nextRequestMessage = RequestMessage.makeRequestMsg(client.getPrivateKey(), requestMessage.getOperation().update());
				}
				if(nextRequestMessage.getTime() < client.getReceivingTimeStamp()){
					return;
				}
				client.setReceivingTimeStamp(nextRequestMessage.getTime());
				BroadcastTask task = new BroadcastTask(nextRequestMessage, this.client, this.timerMap, this.timerCount + 1);
				RequestMessage finalNextRequestMessage = nextRequestMessage;
				client.getReplicaMap().values().forEach(socket -> client.send(socket, finalNextRequestMessage));
				Timer timer = new Timer();
				timer.schedule(task, (this.timerCount + 1) * client.viewChangeTimeOutMillis);

				timerMap.put(nextRequestMessage.getTime(), timer);
			}
		}
	}
}
