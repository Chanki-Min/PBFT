package kr.ac.hongik.apl;

public interface Backup {
    void broadcastPrepareMessage(PreprepareMessage message);

    void broadcastCommit(Message message);

    void broadcastViewChange(Message message);

    void broadcastNewView(Message message);

    void broadcastCheckpoint(Message message);
}
