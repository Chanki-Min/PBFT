package kr.ac.hongik.apl;

import java.io.Serializable;
import java.security.PublicKey;

abstract class Operation implements Serializable {
    final private PublicKey clientInfo;
    final private long timestamp;

    protected Operation(PublicKey clientInfo, long timestamp) {
        this.clientInfo = clientInfo;
        this.timestamp = timestamp;
    }

    abstract Object execute();

    PublicKey getClientInfo() {
        return this.clientInfo;
    }

    long getTimestamp() {
        return this.timestamp;
    }
}
