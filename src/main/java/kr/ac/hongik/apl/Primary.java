package kr.ac.hongik.apl;

public interface Primary extends Backup {
    void broadcastToReplica(RequestMessage message);
}
