package kr.ac.hongik.apl;

import java.io.Serializable;

public class Node implements Serializable {
	Node left = null;
	Node right = null;
	private String hash = null;

	public Node(String hash) {
		this.setHash(hash);
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}
}
