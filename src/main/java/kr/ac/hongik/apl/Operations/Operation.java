package kr.ac.hongik.apl.Operations;

import java.io.Serializable;
import java.security.PublicKey;
import java.time.Instant;
import java.util.UUID;

public abstract class Operation implements Serializable {
    final private PublicKey clientInfo;
    private long timestamp;
    private String random = UUID.randomUUID().toString(); //This random string guarantee uniqueness

	protected Operation(PublicKey clientInfo) {
        this.clientInfo = clientInfo;
        this.timestamp = Instant.now().toEpochMilli();
    }

    public Operation update() {
        timestamp = Instant.now().toEpochMilli();
        random = UUID.randomUUID().toString();
        return this;
    }
    public abstract Object execute(Object obj);

	public PublicKey getClientInfo() {
        return this.clientInfo;
    }

	public long getTimestamp() {
        return this.timestamp;
    }
}
