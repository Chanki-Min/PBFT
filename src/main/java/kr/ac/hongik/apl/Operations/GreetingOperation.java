package kr.ac.hongik.apl.Operations;

import java.security.PublicKey;

public class GreetingOperation extends Operation {
	private String str;

	public GreetingOperation(PublicKey clientInfo) {
		this(clientInfo, "Hello, World!");
	}

	public GreetingOperation(PublicKey clientInfo, String string) {
		super(clientInfo);
		this.str = string;
	}

	@Override
	public Object execute(Object obj) {
		return this.str;
	}
}
