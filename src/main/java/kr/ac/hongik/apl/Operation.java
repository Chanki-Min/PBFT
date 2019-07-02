package kr.ac.hongik.apl;

import java.io.Serializable;
import java.security.PublicKey;
import java.time.Instant;
import java.util.UUID;

abstract class Operation implements Serializable {
    final private PublicKey clientInfo;
    final private long timestamp;
    final private boolean isDistributed;
    final private String random = UUID.randomUUID().toString(); //This random string guarantee uniqueness

    protected Operation(PublicKey clientInfo, boolean isDistributed) {
        this.clientInfo = clientInfo;
        this.timestamp = Instant.now().toEpochMilli();
        this.isDistributed = isDistributed;
    }

    protected Operation(PublicKey clientInfo) {
        this(clientInfo, false);
    }

    public abstract Object execute(Logger logger);

    PublicKey getClientInfo() {
        return this.clientInfo;
    }

    long getTimestamp() {
        return this.timestamp;
    }

    public boolean isDistributed() {
        return this.isDistributed;
    }
}
