package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Util;

import java.io.Serializable;
import java.security.PrivateKey;
import java.security.PublicKey;

import static kr.ac.hongik.apl.Util.sign;

public class CheckPointMessage implements Message {
	private Data data;
	private byte[] signature;

	private CheckPointMessage(Data data, byte[] signature) {
		this.data = data;
		this.signature = signature;
	}

	public static CheckPointMessage makeCheckPointMessage(PrivateKey privateKey, int seqNum, String digest, int replicaNum) {
		Data data = new Data(seqNum, digest, replicaNum);
		byte[] signature = sign(privateKey, data);
		return new CheckPointMessage(data, signature);
	}

	public boolean verify(PublicKey publicKey) {
		return Util.verify(publicKey, this.data, this.signature);
	}

	public byte[] getSignature() {
		return signature;
	}

	public int getSeqNum() {
		return data.seqNum;
	}

	public int getReplicaNum() {
		return data.replicaNum;
	}

	public String getDigest() {
		return data.digest;
	}

	private static class Data implements Serializable {
		private final int seqNum;
		private final int replicaNum;
		private final String digest;

		private Data(int seqNum, String digest, int replicaNum) {
			this.seqNum = seqNum;
			this.digest = digest;
			this.replicaNum = replicaNum;
		}
	}
}