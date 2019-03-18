package kr.ac.hongik.apl;

import java.io.Serializable;
import java.security.PublicKey;

abstract class Operation implements Serializable {
    final private PublicKey clientInfo;
    final private long timestamp;
    final private boolean isDistributed;

    protected Operation(PublicKey clientInfo, long timestamp, boolean isDistributed) {
        this.clientInfo = clientInfo;
        this.timestamp = timestamp;
        this.isDistributed = isDistributed;
    }

    protected Operation(PublicKey clientInfo, long timestamp) {
        this.clientInfo = clientInfo;
        this.timestamp = timestamp;
        isDistributed = false;
    }

	public abstract Object execute();

    PublicKey getClientInfo() {
        return this.clientInfo;
    }

    long getTimestamp() {
        return this.timestamp;
    }

    public boolean isDistributed() {
        return isDistributed;
    }
}
