package kr.ac.hongik.apl;

import java.io.Serializable;
import java.net.InetSocketAddress;

abstract class Operation implements Serializable {
    final private InetSocketAddress clientInfo;
    final private long timestamp;

    protected Operation(InetSocketAddress clientInfo, long timestamp) {
        this.clientInfo = clientInfo;
        this.timestamp = timestamp;
    }

    abstract Result execute();

    InetSocketAddress getClientInfo() {
        return this.clientInfo;
    }

    long getTimestamp() {
        return this.timestamp;
    }
}
