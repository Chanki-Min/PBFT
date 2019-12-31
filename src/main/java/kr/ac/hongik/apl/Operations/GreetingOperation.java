package kr.ac.hongik.apl.Operations;

import java.security.PublicKey;

public class GreetingOperation extends Operation {

	public GreetingOperation(PublicKey clientInfo) {
		super(clientInfo);
	}

	@Override
	public Object execute(Object obj) throws OperationExecutionException {
		try {
			return new Greeting("Hello, World!");
		} catch (Exception e) {
			throw new OperationExecutionException(e);
		}
	}
}
