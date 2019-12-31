package kr.ac.hongik.apl.Operations;

public class OperationExecutionException extends Exception{
	public OperationExecutionException(String s) {
		super(s);
	}

	public OperationExecutionException(Exception e) {
		super(e);
	}

}
