package kr.ac.hongik.apl;

import java.security.PublicKey;

public class GreetingOperation extends Operation {

    protected GreetingOperation(PublicKey clientInfo) {
        super(clientInfo);
    }

    @Override
    public Object execute(Logger logger) {
        return new Greeting("Hello, World!");
    }
}
