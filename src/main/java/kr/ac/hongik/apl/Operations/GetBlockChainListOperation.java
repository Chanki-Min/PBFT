package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.Logger;

import java.security.PublicKey;

public class GetBlockChainListOperation extends Operation {

	public GetBlockChainListOperation(PublicKey clientInfo) {
		super(clientInfo);
	}

	@Override
	public Object execute(Object obj) throws OperationExecutionException {
		return ((Logger) obj).getLoadedChainList();
	}
}
