package kr.ac.hongik.apl;

import java.security.PublicKey;
import java.time.Instant;

public class GreetingOperation extends Operation {

    protected GreetingOperation(PublicKey clientInfo) {
        super(clientInfo, Instant.now().getEpochSecond());
    }

    @Override
	public Object execute() {
        return new Greeting("Hello, World!");
    }
}
