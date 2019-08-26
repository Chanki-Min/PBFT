package kr.ac.hongik.apl.Operations;

import java.security.PublicKey;

public class GreetingOperation extends Operation {

	public GreetingOperation(PublicKey clientInfo) {
		super(clientInfo);
	}

	@Override
	public Object execute(Object obj) {
		return new Greeting("Hello, World!");
	}
}
