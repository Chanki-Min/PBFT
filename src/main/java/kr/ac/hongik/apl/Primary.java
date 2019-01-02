package kr.ac.hongik.apl;

public interface Primary extends Backup {
    void broadcastRequest(Message msg);
}
