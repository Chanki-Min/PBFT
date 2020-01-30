package kr.ac.hongik.apl;

import kr.ac.hongik.apl.Messages.*;
import kr.ac.hongik.apl.Operations.OperationExecutionException;
import org.apache.logging.log4j.LogManager;
import org.echocat.jsu.JdbcUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.PublicKey;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.diffplug.common.base.Errors.rethrow;
import static kr.ac.hongik.apl.Messages.PrepareMessage.makePrepareMsg;
import static kr.ac.hongik.apl.Messages.PreprepareMessage.makePrePrepareMsg;
import static kr.ac.hongik.apl.Messages.ReplyMessage.makeReplyMsg;
import static kr.ac.hongik.apl.Messages.UnstableCheckPoint.makeUnstableCheckPoint;

/**
 * PBFT 알고리즘의 Replica를 구현한 클래스임. Launcher 객체의 main함수에서 호출되어 동작함
 * Client의 RequestMsg를 받아 PBFT알고리즘에 따른 합의 및 요청 실행을 수행함.
 */
public class Replica extends Connector {
	/**
	 * Replica가 WATERMART_UNIT단위의 Execution마다 GC를 실행한다
	 */
	public final static int WATERMARK_UNIT = 10;
	/**
	 * Replica의 성능측정 모드를 켠다
	 */
	final static boolean MEASURE = false;
	/**
	 * Garbage collection과 ViewChangeTimer의 충돌을 방지하는 Lock
	 */
	public final Object watermarkLock = new Object();
	/**
	 * ViewChangeTimer와 Replica.handleNewViewMsg의 충돌을 막기 위한 Lock
	 */
	public final Object viewChangeLock = new Object();

	private int viewNum = 0;
	private final int myNumber;
	private int lowWatermark;

	private Logger logger;
	/**
	 * FIFO 실행을 보장하기 위하여 Committed-Local 단계를 통과한 msg들을 삽입하고, 가장 seqNum이 낮은 Request를 처리함
	 */
	private PriorityQueue<CommitMessage> executionBuffer = new PriorityQueue<>(Comparator.comparingInt(CommitMessage::getSeqNum));
	/**
	 * super.receive한 메세지들의 우선순위대로 처리하기 위핸 버퍼
	 */
	private PriorityBlockingQueue<Message> receiveBuffer = new PriorityBlockingQueue<>(10, Comparator.comparing(Replica::getPriorityFromMsg));

	private ConcurrentHashMap<String, Timer> viewChangeTimerMap = new ConcurrentHashMap<>();
	private ConcurrentHashMap<Integer, Timer> newViewTimerMap = new ConcurrentHashMap<>();
	private AtomicBoolean isViewChangePhase = new AtomicBoolean(false);

	private HashMap<Long, Long> receiveRequestTimeMap = new HashMap<>();
	private HashMap<Long, Long> consensusTimeMap = new HashMap<>();

	public static final org.apache.logging.log4j.Logger msgDebugger = LogManager.getLogger("msgDebugger");
	public static final org.apache.logging.log4j.Logger detailDebugger = LogManager.getLogger("detail");
	public static final org.apache.logging.log4j.Logger measureDebugger = LogManager.getLogger("measure");

	/**
	 * Replica의 main함수이다. args와 properties파일로 Replica 객체를 설정하고 실행시키는 역할을 한다
	 *
	 * @param args String array, {0: 외부IP, 1: 포트포워드된 외부 포트, 2: 내부 포트} 의 형식이어야 하며, 포트포워드를 하지 않을 경우 2,3은 같아야 한다
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		try {
			Properties properties = new Properties();
			//InputStream is = Replica.class.getResourceAsStream("/replica.properties");
			InputStream is = Util.getInputStreamOfGivenResource("replica.properties");
			properties.load(new java.io.BufferedInputStream(is));
			if(args.length < 3) {
				msgDebugger.error(String.format("Usage: program <ip> <forwarded-public port> <opening port>"));
				return;
			}
			String outerIP = args[0];
			int outerPort = Integer.parseInt(args[1]);
			int innerPort = Integer.parseInt(args[2]);

			Replica replica = new Replica(properties, outerIP, outerPort, innerPort);
			replica.start();
		} catch (Exception e) {
			msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * Replica 클래스의 생성자 메소드, param을 받아 객체의 설정, receive 포트를 열기, 알고 있는 레플리카와 연결을 시도한다.
	 *
	 * @param prop replica.properties 객체
	 * @param serverPublicIp 서버의 외부IP (prop에서 원하는 정보를 얻기 위하여 필요하다)
	 * @param serverPublicPort 서버의 외부Port (prop에서 원하는 정보를 얻기 위하여 필요하다)
	 * @param serverInnerPort 서버의 실제 Port (receive socket을 열기 위하여 필요하다)
	 */
	public Replica(Properties prop, String serverPublicIp, int serverPublicPort, int serverInnerPort) {
		super(prop); //Connector 설정
		this.logger = new Logger(serverPublicIp, serverPublicPort); //logger file 생성
		this.myNumber = getMyNumberFromProperty(prop, serverPublicIp, serverPublicPort);
		this.lowWatermark = 0;

		try {
			ServerSocketChannel listener = ServerSocketChannel.open();
			listener.socket().setReuseAddress(true);
			listener.configureBlocking(false);
			listener.bind(new InetSocketAddress(serverInnerPort));
			listener.register(this.selector, SelectionKey.OP_ACCEPT); //내부 포트를 소켓에 바인딩하고 Connector가 사용할수 있도록 register
		} catch (IOException e) {
			msgDebugger.error(e);
			throw new Error(e);
		}
		super.connect(); //모든 레플리카와 연결 시도
	}

	/**
	 * replica 객체를 시작하며 2가지 쓰레드를 활성화한다.
	 *
	 * 1. (새로운 쓰레드) 루프를 돌며 Connector.receiver()로 새로운 메세지를 받아서 receiverBuffer에 put
	 * 2. (main 쓰레드) receiveBuffer에서 poll한 메세지를 처리한다
	 *
	 * 이러한 구조를 통하여 TCP 버퍼가 꽉 차서 메세지가 버려지는 것을 방지하고, 우선순위에 따른 메세지 처리를 수행한다.
	 */
	private void start() {
		//1번
		new Thread(() -> {
			while (true) {
				try {
					receiveBuffer.put(super.receive());
				} catch (InterruptedException ignored) {
				}
			}
		}).start();

		//2번
		while (true) {
			Message message;
			try {
				message = receive();
			} catch (InterruptedException e) {
				msgDebugger.error("Interrupted");
				continue;
			}
			if (message instanceof HeaderMessage) {
				handleHeaderMessage((HeaderMessage) message);
			} else if(message instanceof  CloseConnectionMessage) {
				handleCloseConnectionMessage((CloseConnectionMessage) message);
			} else if (message instanceof RequestMessage) {
				if (!publicKeyMap.containsValue(((RequestMessage) message).getOperation().getClientInfo())) {
					detailDebugger.trace(String.format("Got request msg but ignored, publicKeyMap.contains? : %b", publicKeyMap.containsValue(((RequestMessage) message).getClientInfo())));
					receiveBuffer.put(message);
				}
				else
					handleRequestMessage((RequestMessage) message);
			} else if (message instanceof PreprepareMessage) {
				if (!publicKeyMap.containsValue(((PreprepareMessage) message).getOperation().getClientInfo())) {
					receiveBuffer.put(message);
					detailDebugger.trace(String.format("Got request msg but ignored, publicKeyMap.contains? : %b", publicKeyMap.containsValue(((PreprepareMessage) message).getRequestMessage().getClientInfo())));
				}
				else
					handlePreprepareMessage((PreprepareMessage) message);
			} else if (message instanceof PrepareMessage) {
				handlePrepareMessage((PrepareMessage) message);
			} else if (message instanceof CommitMessage) {
				handleCommitMessage((CommitMessage) message);
			} else if (message instanceof CheckPointMessage) {
				handleCheckPointMessage((CheckPointMessage) message);
			} else if (message instanceof ViewChangeMessage) {
				handleViewChangeMessage((ViewChangeMessage) message);
			} else if (message instanceof NewViewMessage) {
				handleNewViewMessage((NewViewMessage) message);
			} else {
				throw new UnsupportedOperationException("Invalid message");
			}
		}
	}

	/**
	 * receiveBuffer에서 우선순위 및 조건을 만족하는 메세지를 받아온다
	 *
	 * @return 우선순위가 가장 높으며 조건을 만족하는 Message
	 * @throws InterruptedException
	 */
	@Override
	protected Message receive() throws InterruptedException {
		Message message;
		/**
		 * 현재 WaterMark range보다 높은 seqNum을 가진 Preprepare, Prepare Msg는 리턴해서는 안된다 (handling method에서 버려짐)
		 */
		Class[] waterMarkCheckTypes = new Class[] {PreprepareMessage.class, PrepareMessage.class};
		/**
		 * 만약 지금 ViewChangePhase가 true이면 아래의 Msg들만 통과시켜야 한다
		 */
		Class[] viewChangeUnblockTypes = new Class[] {CheckPointMessage.class, ViewChangeMessage.class, NewViewMessage.class, HeaderMessage.class };

		Predicate<Message> isWaterMarkType = (msg) -> Arrays.stream(waterMarkCheckTypes).anyMatch(msgType -> msgType.isInstance(msg));
		Predicate<Message> isBlockType = (msg) -> Arrays.stream(viewChangeUnblockTypes).noneMatch(msgType -> msgType.isInstance(msg));

		while (true) {
			message = receiveBuffer.take();
			if (isViewChangePhase.get()) {
				if (isBlockType.test(message)) {
					receiveBuffer.offer(message);
					continue;
				}
				return message;
			} else {
				if (!isWaterMarkType.test(message)) {
					return message;
				} else {
					int SeqNum = getSeqNumFromMsg(message);
					if (SeqNum < this.getWatermarks()[0]) {
						//Msg가 waterMark 보다 작을 경우 이미 처리된 메세지이므로 버린다
					} else if (SeqNum < this.getWatermarks()[1]) {
						return message;
					} else {
						receiveBuffer.offer(message);
					}
				}
			}
		}
	}

	@Override
	protected void sendHeaderMessage(SocketChannel channel) {
		HeaderMessage headerMessage = new HeaderMessage(this.myNumber, this.getPublicKey(), "replica");
		send(channel, headerMessage);
	}

	private void handleRequestMessage(RequestMessage message) {

		msgDebugger.debug(String.format("Got Request %d", message.getTime()));

		ReplyMessage replyMessage = findReplyMessageOrNull(message);
		if (replyMessage != null) {
			SocketChannel destination = getChannelFromClientInfo(replyMessage.getClientInfo());
			send(destination, replyMessage);
			return;
		}

		detailDebugger.trace("not in reply");

		if (this.logger.findMessage(message)) {
			return;
		}

		detailDebugger.trace("not in request");

		var sock = getChannelFromClientInfo(message.getClientInfo());
		PublicKey publicKey = publicKeyMap.get(sock);
		boolean canGoNextState = message.verify(publicKey) &&
				message.isNotRepeated(logger::getPreparedStatement);
		if (canGoNextState) {
			detailDebugger.trace("cangoNextState");
			if(MEASURE){
				receiveRequestTimeMap.put(message.getTime(), Instant.now().toEpochMilli());
			}
			logger.insertMessage(message);
			if (this.getPrimary() == this.myNumber) {
				detailDebugger.trace("broadcastToReplica");
				broadcastToReplica(message);
			} else {
				detailDebugger.trace(String.format("relay to primary, viewNum : %d", viewNum));
				super.send(getReplicaMap().get(getPrimary()), message);

				//Set a timer for view-change phase
				ViewChangeTimerTask viewChangeTimerTask = new ViewChangeTimerTask(getWatermarks()[0], this.getViewNum() + 1, this);
				Timer timer = new Timer();
				timer.schedule(viewChangeTimerTask, Replica.TIMEOUT);

				/** Store timer object to cancel it when the request is executed and the timer is not expired.
				 * key: operation's hash, value: timer
				 * An operation can be a key because every operation has a random UUID;
				 */
				String key = makeKeyForTimer(message);
				viewChangeTimerMap.put(key, timer);
			}
		}
	}

	private void handlePreprepareMessage(PreprepareMessage message) {

		msgDebugger.debug(String.format("Got Pre-pre msg view : %d seq : %d request timestamp : %d", message.getViewNum(), message.getSeqNum(), message.getRequestMessage().getTime()));

		SocketChannel primaryChannel = getReplicaMap().get(this.getPrimary());
		PublicKey primaryPublicKey = this.publicKeyMap.get(primaryChannel);
		PublicKey clientPublicKey = message.getRequestMessage().getClientInfo();

		boolean isVerified = message.isVerified(primaryPublicKey, this.getViewNum(), clientPublicKey,logger::getPreparedStatement);

		if (isVerified) {
			msgDebugger.debug(String.format("Pre-pre msg verified view : %d seq : %d request timestamp : %d", message.getViewNum(), message.getSeqNum(), message.getRequestMessage().getTime()));
			logger.insertMessage(message);
			logger.insertMessage(message.getRequestMessage());
			PrepareMessage prepareMessage = makePrepareMsg(
					this.getPrivateKey(),
					message.getViewNum(),
					message.getSeqNum(),
					message.getDigest(),
					this.myNumber);
			logger.insertMessage(prepareMessage);
			getReplicaMap().values().forEach(channel -> send(channel, prepareMessage));
		}
	}

	private void handlePrepareMessage(PrepareMessage message) {

		msgDebugger.debug(String.format("Got Prepare msg view : %d seq : %d from %d", message.getViewNum(), message.getSeqNum(), message.getReplicaNum()));

		PublicKey publicKey = publicKeyMap.get(getReplicaMap().get(message.getReplicaNum()));
		if (message.isVerified(publicKey, this.getViewNum(), this::getWatermarks)) {
			logger.insertMessage(message);
		}

		/*
		* Commits 의 viewNum까지 확인해줘야 viewChange phase에서 재접속한 노드가 정상적인 실행을 할 수 있음에 주의해야 한다.
		*/
		try (var pstmt = logger.getPreparedStatement(Logger.CONSENSUS,"SELECT count(*) FROM Commits C WHERE C.viewNum = ? ANd C.seqNum = ? AND C.replica = ? AND C.digest = ?")) {
			pstmt.setInt(1, message.getViewNum());
			pstmt.setInt(2, message.getSeqNum());
			pstmt.setInt(3, this.myNumber);
			pstmt.setString(4, message.getDigest());
			try (var ret = pstmt.executeQuery()) {
				if (ret.next()) {
					var i = ret.getInt(1);
					if (i > 0)
						return;
				}
			}
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}

		if (message.isPrepared(logger::getPreparedStatement, getMaximumFaulty(), this.myNumber)) {
			CommitMessage commitMessage = CommitMessage.makeCommitMsg(
					this.getPrivateKey(),
					message.getViewNum(),
					message.getSeqNum(),
					message.getDigest(),
					this.myNumber);

			//prepare 메세지를 2f+1개 초과해서 받을때 중복된 Commit msg생성을 방지하기 위하여 일단 내가 만든 커밋 메세지를 핸들한다.
			handleCommitMessage(commitMessage);
			getReplicaMap().values().forEach(channel -> send(channel, commitMessage));
		}
	}

	private void handleHeaderMessage(HeaderMessage message) {
		msgDebugger.debug(String.format("Got Header msg replica : %d channel : %s", message.getReplicaNum(), message.getChannel().toString()));

		SocketChannel channel = message.getChannel();
		this.publicKeyMap.put(channel, message.getPublicKey());

		switch (message.getType()) {
			case "replica":
				getReplicaMap().put(message.getReplicaNum(), channel);
				break;
			case "client":
				break;
			default:
				msgDebugger.error(String.format("Invalid header message : %s", message));
		}
	}

	private void handleCloseConnectionMessage(CloseConnectionMessage message) {
		detailDebugger.trace(String.format("Got CloseConnection msg, replica : %d channel : %s", message.getReplicaNum(), message.getChannel().toString()));
		SocketChannel channel = message.getChannel();
		this.publicKeyMap.remove(channel);
		closeWithoutException(channel);
		msgDebugger.debug(String.format("Connection Closed from channel : %s", message.getChannel().toString()));
	}

	private void handleCommitMessage(CommitMessage cmsg) {

		msgDebugger.debug(String.format("Got Commit msg view : %d seq : %d from %d", cmsg.getViewNum(), cmsg.getSeqNum(), cmsg.getReplicaNum()));

		PublicKey publicKey = publicKeyMap.get(getReplicaMap().get(cmsg.getReplicaNum()));
		if (!cmsg.verify(publicKey))
			return;
		logger.insertMessage(cmsg);
		boolean isCommitted = cmsg.isCommittedLocal(logger::getPreparedStatement, getMaximumFaulty(), this.myNumber);

		if (isCommitted) {
			if(MEASURE){
				Replica.detailDebugger.trace("isCommitted");
				Long key = logger.getOperation(cmsg).getTimestamp();
				consensusTimeMap.put(key, Instant.now().toEpochMilli() - receiveRequestTimeMap.get(key));
				double duration = (double)(consensusTimeMap.get(key)) / 1000;

				measureDebugger.info(String.format("Consensus Completed #%d\t%f sec\n", cmsg.getSeqNum(), duration));
			}
			if (executionBuffer.stream().noneMatch(x -> x.getSeqNum() == cmsg.getSeqNum()))
				executionBuffer.add(cmsg);
			try {
				while (true) {
					Replica.detailDebugger.trace("try to getRightNextCommitMsg");
					CommitMessage rightNextCommitMsg = getRightNextCommitMsg();
					Replica.detailDebugger.trace("got getRightNextCommitMsg");
					var operation = logger.getOperation(rightNextCommitMsg);
					// Release backup's view-change timer
					String key = makeKeyForTimer(rightNextCommitMsg);
					Timer timer = viewChangeTimerMap.remove(key);
					Optional.ofNullable(timer).ifPresent(Timer::cancel);

					if (operation != null) {
						Object ret;
						try {
							ret = operation.execute(this.logger);
							msgDebugger.info(String.format("Executed #%d", rightNextCommitMsg.getSeqNum()));
						} catch (OperationExecutionException e) {
							Replica.msgDebugger.error("Error occurred in Execution phase. ", e);
							ret = e;	//operation의 실행 중 예외 발생시 예외 객체를 reply로 보낸다.
						}
						if(MEASURE) {
							if (rightNextCommitMsg.getSeqNum()%10 == 0) {
								printConsensusTime();
							}
						}
						var viewNum = cmsg.getViewNum();
						var timestamp = operation.getTimestamp();
						var clientInfo = operation.getClientInfo();
						ReplyMessage replyMessage = makeReplyMsg(getPrivateKey(), viewNum, timestamp,
								clientInfo, myNumber, ret);

						logger.insertMessage(rightNextCommitMsg.getSeqNum(), replyMessage);
						SocketChannel destination = getChannelFromClientInfo(replyMessage.getClientInfo());
						send(destination, replyMessage);

						UnstableCheckPoint unstableCheckPoint = makeUnstableCheckPoint(getPrivateKey(), this.getWatermarks()[0], rightNextCommitMsg.getSeqNum(), rightNextCommitMsg.getDigest());
						logger.insertMessage(unstableCheckPoint);
					}

					/* Checkpoint Phase */
					if (rightNextCommitMsg.getSeqNum() == getWatermarks()[1] - 1) {

						int seqNum = rightNextCommitMsg.getSeqNum();
						CheckPointMessage checkpointMessage = CheckPointMessage.makeCheckPointMessage(
								this.getPrivateKey(),
								seqNum,
								logger.getStateDigest(seqNum, getMaximumFaulty(), rightNextCommitMsg.getViewNum()),
								this.myNumber);
						//Broadcast message
						getReplicaMap().values().forEach(sock -> send(sock, checkpointMessage));
					}
				}
			} catch (NoSuchElementException ignored) {
			}
		}
	}

	private void handleCheckPointMessage(CheckPointMessage message) {

		msgDebugger.debug(String.format("Got Checkpoint msg seq : %d from %d", message.getSeqNum(), message.getReplicaNum()));

		PublicKey publicKey = publicKeyMap.get(getReplicaMap().get(message.getReplicaNum()));
		if (!message.verify(publicKey))
			return;

		logger.insertMessage(message);

		if (message.getSeqNum() < getWatermarks()[0]) {
			return;
		}
		try {
			String query = "SELECT count(*) FROM Checkpoints C WHERE C.seqNum = ? AND C.replica = ?";
			try (var psmt = logger.getPreparedStatement(Logger.CONSENSUS, query)) {
				psmt.setInt(1, message.getSeqNum());
				psmt.setInt(2, this.myNumber);
				var ret = psmt.executeQuery();
				ret.next();
				if (ret.getInt(1) != 1)
					return;
			}
			query = "SELECT stateDigest FROM Checkpoints C WHERE C.seqNum = ?";
			try (var psmt = logger.getPreparedStatement(Logger.CONSENSUS, query)) {
				psmt.setInt(1, message.getSeqNum());
				try (var ret = psmt.executeQuery()) {
					Map<String, Integer> digestMap = new HashMap<>();
					while (ret.next()) {
						var key = ret.getString(1);
						var num = digestMap.getOrDefault(key, 0);
						digestMap.put(key, num + 1);
					}
					int max = digestMap
							.values()
							.stream()
							.max(Comparator.comparingInt(x -> x))
							.orElse(0);

					if (max > 2 * getMaximumFaulty() && message.getSeqNum() > this.lowWatermark) {
						synchronized (watermarkLock) {
							List<String> clientInfos = publicKeyMap.values().stream()
									.map(Util::serToBase64String)
									.collect(Collectors.toList());
							logger.executeGarbageCollection(message.getSeqNum(), clientInfos);
							lowWatermark += WATERMARK_UNIT;

							detailDebugger.trace(String.format("GARBAGE COLLECTION DONE -> new low watermark : %d", lowWatermark));

						}
					}
				}
			}
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private void handleViewChangeMessage(ViewChangeMessage message) {

		msgDebugger.debug(String.format("Got ViewChange msg NEWVIEW : %d from %d", message.getNewViewNum(), message.getReplicaNum()));

		PublicKey publicKey = publicKeyMap.get(getReplicaMap().get(message.getReplicaNum()));
		if (!message.isVerified(publicKey, this.getMaximumFaulty(), WATERMARK_UNIT)) {
			return;
		}
		try {
			String isAlreadyInsertedQuery = "SELECT count(*) FROM Viewchanges V WHERE V.newViewNum = ? AND V.replica = ?";
			try (var pstmt = logger.getPreparedStatement(Logger.CONSENSUS, isAlreadyInsertedQuery)) {
				pstmt.setInt(1, message.getNewViewNum());
				pstmt.setInt(2, message.getReplicaNum());
				ResultSet rs = pstmt.executeQuery();
				rs.next();
				if (rs.getInt(1) != 0) {
					return;
				}
			}
			logger.insertMessage(message);
			if (this.getViewNum() >= message.getNewViewNum()) {
				return;
			}

			if (message.getNewViewNum() % getReplicaMap().size() == getMyNumber() && canMakeNewViewMessage(message)) {
				/* 정확히 2f + 1개일 때만 broadcast */

				msgDebugger.debug("Send NewViewMessage");
				NewViewMessage newViewMessage = NewViewMessage.makeNewViewMessage(this, message.getNewViewNum());
				logger.insertMessage(newViewMessage);
				getReplicaMap().values().forEach(sock -> send(sock, newViewMessage));
				//handleNewViewMessage(newViewMessage);
			} else if (hasTwoFPlusOneMessages(message)) {
				/* 2f + 1개의 v+i에 해당하는 메시지 수집 -> new view를 기다리는 동안 timer 작동 */
				ViewChangeTimerTask viewChangeTimerTask = new ViewChangeTimerTask(getWatermarks()[0], message.getNewViewNum() + 1, this);
				Timer timer = new Timer();
				timer.schedule(viewChangeTimerTask, TIMEOUT * (message.getNewViewNum() + 1  - this.getViewNum()));

				newViewTimerMap.put(message.getNewViewNum() + 1, timer);
				msgDebugger.debug(String.format("Put NewViewTimer in timerMap, timerMap size : %d", viewChangeTimerMap.size()));
			} else {
				/* f + 1 이상의 v > currentView 인 view-change를 수집한다면
					나 자신도 f + 1개의 view-change 중 min-view number로 view-change message를 만들어 배포한다. */

				try (var pstmt = logger.getPreparedStatement(Logger.CONSENSUS, isAlreadyInsertedQuery)) {
					pstmt.setInt(1, message.getNewViewNum());
					pstmt.setInt(2, getMyNumber());
					ResultSet rs = pstmt.executeQuery();
					rs.next();
					if (rs.getInt(1) != 0) {
						return;
					}
				}
				List<Integer> newViewNumList;
				String query = "SELECT V1.newViewNum FROM Viewchanges V1 "
						+ "WHERE V1.newViewNum > ? AND "
						+ "NOT V1.newViewNum IN "
						+ "( SELECT V2.newViewNum FROM Viewchanges V2 WHERE V2.replica = ? )";
				try (var pstmt = logger.getPreparedStatement(Logger.CONSENSUS, query)) {
					pstmt.setInt(1, this.getViewNum());
					pstmt.setInt(2, this.getMyNumber());
					ResultSet rs = pstmt.executeQuery();
					newViewNumList = JdbcUtils.toStream(rs)
							.map(rethrow().wrapFunction(x -> x.getString(1)))
							.map(Integer::valueOf)
							.sorted()
							.collect(Collectors.toList());
				}
				int minNewViewNum = newViewNumList.stream().min(Integer::min).get();

				if (newViewNumList.size() == getMaximumFaulty() + 1) {
					this.setViewChangePhase(true);

					message.getCheckPointMessages().stream().forEach(x -> logger.insertMessage(x));
					synchronized (watermarkLock) {
						removeViewChangeTimer();
						ViewChangeMessage viewChangeMessage = ViewChangeMessage.makeViewChangeMsg(
								message.getLastCheckpointNum(), minNewViewNum, this,
								logger::getPreparedStatement);
						getReplicaMap().values().forEach(sock -> send(sock, viewChangeMessage));
					}
				}
			}
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private void handleNewViewMessage(NewViewMessage message) {

		msgDebugger.debug(String.format("Got Newview msg from %d, time : %d", message.getNewViewNum(), System.currentTimeMillis()));
		if(message.getNewViewNum()%getReplicaMap().size() == getMyNumber()) {
			msgDebugger.info(String.format("I'm New Primary"));
		}

		if (!message.isVerified(this) || message.getNewViewNum() <= this.getViewNum()) {
			return;
		}
		this.logger.insertMessage(message);

		removeViewChangeTimer();
		removeNewViewTimer(message.getNewViewNum());
		synchronized (viewChangeLock) {
			setViewChangePhase(false);
			setViewNum(message.getNewViewNum());
		}
		int newLowWatermark = message.getViewChangeMessageList()
				.stream()
				.max(Comparator.comparingInt(ViewChangeMessage::getLastCheckpointNum))
				.get()
				.getLastCheckpointNum();

		synchronized (watermarkLock) {
			this.lowWatermark = getWatermarks()[0] > newLowWatermark ? getWatermarks()[0] : newLowWatermark;
			message.getViewChangeMessageList()
					.stream()
					.flatMap(x -> x.getCheckPointMessages().stream())
					.forEach(c -> logger.insertMessage(c));
			List<String> clientInfos = publicKeyMap.values().stream()
					.map(Util::serToBase64String)
					.collect(Collectors.toList());
			logger.executeGarbageCollection(getWatermarks()[0] - 1, clientInfos);
		}
		//모든 preprepareMsg를 합의하여 낙오된 replica를 복구한다
		if (message.getOperationList() != null) {
			message.getOperationList()
					.stream()
					.forEach(pp -> handlePreprepareMessage(pp));
		}
		msgDebugger.info(String.format("Newview Fixed, TimerMap Size : %d",this.viewChangeTimerMap.size()));
	}

	/**
	 * Generate key for getting timer from timerMap.
	 * Commit message doesn't have an operation, so the replica need to query from logger.
	 *
	 * @param msg
	 * @return Hash(operation) which is queried from cmsg's request, or request's operation
	 */
	private String makeKeyForTimer(Message msg) {
		if(msg instanceof RequestMessage) {
			return Util.hash( ((RequestMessage) msg).getOperation());
		} else if(msg instanceof CommitMessage) {
			var operation = logger.getOperation((CommitMessage) msg);
			return Util.hash(operation);
		} else {
			msgDebugger.error(String.format("makeKeyForTimer:: cannot make key from %s", msg.getClass().toString()));
			throw new NoSuchElementException(msg.getClass().toString());
		}
	}

	private ReplyMessage findReplyMessageOrNull(RequestMessage requestMessage) {
		try (var pstmt = logger.getPreparedStatement(Logger.CONSENSUS,"SELECT PP.seqNum FROM PrePrepares PP WHERE PP.requestMessage = ?")) {
			pstmt.setString(1, Util.serToBase64String(requestMessage));
			var ret = pstmt.executeQuery();
			if (ret.next()) {
				int seqNum = ret.getInt(1);

				try (var pstmt1 = logger.getPreparedStatement(Logger.CONSENSUS, "SELECT E.replyMessage FROM Executed E WHERE E.seqNum = ?")) {
					pstmt1.setInt(1, seqNum);
					var ret1 = pstmt1.executeQuery();
					if (ret1.next())
						return Util.desToObject(ret1.getString(1), ReplyMessage.class);
					else
						return null;
				}
			} else {
				return null;
			}
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private void broadcastToReplica(RequestMessage message) {
		int seqNum = getLatestSequenceNumber() + 1;
		PreprepareMessage preprepareMessage = makePrePrepareMsg(getPrivateKey(), getViewNum(), seqNum, message);
		logger.insertMessage(preprepareMessage);
		getReplicaMap().values().forEach(channel -> send(channel, preprepareMessage));
	}

	private static int getSeqNumFromMsg(Message message) {
		if (message instanceof HeaderMessage) {
			return -4;
		} else if (message instanceof NewViewMessage) {
			return -3;
		} else if (message instanceof ViewChangeMessage) {
			return -2;
		} else if (message instanceof RequestMessage) {
			return -1;
		} else if (message instanceof PreprepareMessage) {
			return ((PreprepareMessage) message).getSeqNum();
		} else if (message instanceof PrepareMessage) {
			return ((PrepareMessage) message).getSeqNum();
		} else if (message instanceof CommitMessage) {
			return ((CommitMessage) message).getSeqNum();
		} else if (message instanceof CheckPointMessage) {
			return ((CheckPointMessage) message).getSeqNum();
		} else {
			throw new NoSuchElementException("getSeqNumFromMsg can't apply to " + message.getClass().toString());
		}
	}
	/*
		getPriorityFromMsg
		receive 함수에서 getSeqNumFromMsg 로 poll 우선순위를 정하는 부분에서 에러 발생하여 우선순위를 뽑아내는 함수 추가
		4개 서버 중 2대가 view 1에서 view 1, seq 1인 상태에서 request msg를 execute하고 남은 2대가 viewchange 후에 view2에서 view 2 seq 1인 commit msg를 먼저
		뽑고 execute하려면 데드락 발생
		그래서 priority에 viewnum을 반영하여 뽑아올 수 있도록 설정
	*/
	private static int getPriorityFromMsg(Message message) {
		if (message instanceof HeaderMessage) {
			return -5;
		} else if (message instanceof NewViewMessage) {
			return -4;
		} else if (message instanceof ViewChangeMessage) {
			return -3;
		} else if (message instanceof RequestMessage) {
			return -2;
		} else if (message instanceof CloseConnectionMessage) {
			return -1;
		}
		else if (message instanceof PreprepareMessage) {
			return ((PreprepareMessage) message).getSeqNum() + ((PreprepareMessage) message).getViewNum();
		} else if (message instanceof PrepareMessage) {
			return ((PrepareMessage) message).getSeqNum() + ((PrepareMessage) message).getViewNum();
		} else if (message instanceof CommitMessage) {
			return ((CommitMessage) message).getSeqNum() + ((CommitMessage) message).getViewNum();
		} else if (message instanceof CheckPointMessage) {
			return ((CheckPointMessage) message).getSeqNum();
		}
		throw new NoSuchElementException("getSeqNumFromMsg can't apply to " + message.getClass().toString());
	}

	private int getMyNumberFromProperty(Properties prop, String serverIp, int serverPort) {
		int numOfReplica = Integer.parseInt(prop.getProperty("replica"));
		for (int i = 0; i < numOfReplica; i++) {
			if (prop.getProperty("replica" + i).equals(serverIp + ":" + serverPort)) {
				return i;
			}
		}
		throw new RuntimeException("Unauthorized replica");
	}

	private CommitMessage getRightNextCommitMsg() throws NoSuchElementException {
		String query = "SELECT E.seqNum FROM Executed E";
		try (var psmt = logger.getPreparedStatement(Logger.CONSENSUS, query)) {
			try (var ret = psmt.executeQuery()) {
				List<Integer> seqList = JdbcUtils.toStream(ret)
						.map(rethrow().wrapFunction(x -> x.getInt(1)))
						.collect(Collectors.toList());

				int soFarMaxSeqNum = seqList.isEmpty() ? getWatermarks()[0] - 1 : seqList.stream().max(Integer::compareTo).get();
				if (soFarMaxSeqNum < getWatermarks()[0])
					soFarMaxSeqNum = getWatermarks()[0] - 1;
				while(true) {
					var first = executionBuffer.peek();
					if (first != null){
						if(soFarMaxSeqNum + 1 == first.getSeqNum()){
							executionBuffer.poll();
							return first;
						} else if(first.getSeqNum() < getWatermarks()[0]){
							executionBuffer.poll();
						} else {
							Replica.detailDebugger.trace(String.format("getRightNextCommitMsg:: cannot poll from exeBuff, first seq : %d sofar : %d", first.getSeqNum(), soFarMaxSeqNum));
							throw new NoSuchElementException();
						}
					} else {
						Replica.detailDebugger.trace(String.format("getRightNextCommitMsg:: cannot poll from exeBuff, first is null"));
						throw new NoSuchElementException();
					}
				}
			}
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private void loopback(Message message) {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				send(getReplicaMap().get(getMyNumber()), message);
			}
		};
		new Timer().schedule(task, 10000);
	}

	public Map<String, Timer> getViewChangeTimerMap() {
		return viewChangeTimerMap;
	}

	public SocketChannel getChannelFromClientInfo(PublicKey key) {
		/*
		if(!publicKeyMap.containsKey(key))
			return null;
		*/
		return publicKeyMap.entrySet().stream()
				.filter(x -> x.getValue().equals(key))
				.findFirst().get().getKey();
	}

	protected int getLatestSequenceNumber() {
		String query = "SELECT P.seqNum FROM Preprepares P where viewNum = ?";
		try (var pstmt = logger.getPreparedStatement(Logger.CONSENSUS, query)) {
			pstmt.setInt(1, this.getViewNum());
			ResultSet ret = pstmt.executeQuery();
			List<Integer> seqList = JdbcUtils.toStream(ret)
					.map(rethrow().wrapFunction(x -> x.getInt(1)))
					.collect(Collectors.toList());
			return seqList.isEmpty() ? getWatermarks()[0] - 1 : seqList.stream().max(Integer::compareTo).get();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @param message View-change message
	 * @return true if DB has f + 1 same view number messages else false
	 */
	private boolean hasTwoFPlusOneMessages(ViewChangeMessage message) {
		String query = "SELECT newViewNum FROM Viewchanges V WHERE V.newViewNum = ? ";
		try (var pstmt = logger.getPreparedStatement(Logger.CONSENSUS, query)) {
			pstmt.setInt(1, message.getNewViewNum());
			var ret = pstmt.executeQuery();

			return JdbcUtils.toStream(ret)
					.map(rethrow().wrapFunction(row -> row.getString(1)))
					.map(Integer::valueOf)
					.count() == 2 * getMaximumFaulty() + 1;

		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	private boolean canMakeNewViewMessage(ViewChangeMessage message) {
		try {
			Boolean[] checklist = new Boolean[3];
			String query1 = "SELECT count(*) FROM ViewChanges V WHERE V.replica = ? AND V.newViewNum = ?";
			try (var pstmt = logger.getPreparedStatement(Logger.CONSENSUS, query1)) {
				pstmt.setInt(1, this.myNumber);
				pstmt.setInt(2, message.getNewViewNum());
				var ret = pstmt.executeQuery();
				if (ret.next())
					checklist[0] = ret.getInt(1) == 1;
			}
			String query2 = "SELECT count(*) FROM ViewChanges V WHERE V.replica <> ? AND V.newViewNum = ?";
			try (var pstmt = logger.getPreparedStatement(Logger.CONSENSUS, query2)) {
				pstmt.setInt(1, this.myNumber);
				pstmt.setInt(2, message.getNewViewNum());
				var ret = pstmt.executeQuery();
				if (ret.next())
					checklist[1] = ret.getInt(1) >= 2 * getMaximumFaulty();
			}
			String query3 = "SELECT count(*) FROM NewViewMessages WHERE newViewNum = ?";
			try (var psmt = logger.getPreparedStatement(Logger.CONSENSUS, query3)) {
				psmt.setInt(1, message.getNewViewNum());
				var ret = psmt.executeQuery();
				if (ret.next())
					checklist[2] = ret.getInt(1) == 0;
			}
			return Arrays.stream(checklist).allMatch(x -> x);
		} catch (SQLException e) {
			Replica.msgDebugger.error(e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * newViewNum+1 (newViewTimer 만료시 view가 될 숫자) 이하의 newViewTimer들을 전부 삭제한다.
	 * 왜냐하면 이 메소드가 호출된다는 것은 newViewNum+1로 viewChange가 된 것이기 때문이다
	 *
	 * @param newViewNum
	 */
	public void removeNewViewTimer(int newViewNum) {
		List<Integer> deletableKeys = newViewTimerMap.keySet().stream().filter(x -> x <= newViewNum+1).collect(Collectors.toList());
		deletableKeys.forEach(x -> newViewTimerMap.get(x).cancel());
		deletableKeys.forEach(x -> newViewTimerMap.remove(x));
	}

	/**
	 * 모든 viewChangeTimer를 cancel하고 map에서 삭제합니다.
	 */
	public void removeViewChangeTimer() {
		viewChangeTimerMap.entrySet().stream().
				peek(e -> e.getValue().cancel()).
				forEach(e-> viewChangeTimerMap.remove(e.getKey()));
	}
	public void printConsensusTime(){
		if(MEASURE) { double avg = consensusTimeMap
				.values()
				.stream()
				.mapToLong(Long::longValue)
				.average()
				.orElse(0);
			measureDebugger.info(String.format("Average Consensus Time : ", avg/1000));
		}
	}

	public Logger getLogger() {
		return this.logger;
	}

	public int getMyNumber() {
		return this.myNumber;
	}

	public void setViewChangePhase(boolean viewChangePhase) {
		isViewChangePhase.set(viewChangePhase);
	}

	public int getPrimary() {
		return viewNum % getReplicaMap().size();
	}

	/**
	 * @return An array which contains (low watermark, high watermark).
	 */
	public int[] getWatermarks() {
		return new int[] {this.lowWatermark, this.lowWatermark + WATERMARK_UNIT};
	}

	public int getViewNum() {
		return viewNum;
	}

	public void setViewNum(int primary) {
		this.viewNum = primary;
	}
}