package kr.ac.hongik.apl.Operations;

import java.io.Serializable;

public class Greeting implements Serializable {
	public String greeting;

	public Greeting(String greeting) {
		this.greeting = greeting;
	}

	@Override
	public String toString() {
		return this.greeting;
	}
}