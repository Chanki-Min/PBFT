package kr.ac.hongik.apl;

public interface Backup {
    void broadcastPrepare(Message message);

    void broadcastCommit(Message message);

    void broadcastViewChange(Message message);

    void broadcastNewView(Message message);

    void broadcastCheckpoint(Message message);
}
