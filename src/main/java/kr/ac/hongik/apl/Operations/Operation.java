package kr.ac.hongik.apl.Operations;

import java.io.Serializable;
import java.security.PublicKey;
import java.time.Instant;
import java.util.UUID;

public abstract class Operation implements Serializable {
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

    public abstract Object execute(Object obj);

	public PublicKey getClientInfo() {
        return this.clientInfo;
    }

	public long getTimestamp() {
        return this.timestamp;
    }

    public boolean isDistributed() {
        return this.isDistributed;
    }
}
