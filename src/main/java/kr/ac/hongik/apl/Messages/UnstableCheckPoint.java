package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Util;

import java.io.Serializable;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Arrays;

import static kr.ac.hongik.apl.Util.sign;

public class UnstableCheckPoint implements Message {
	private Data data;
	private byte[] signature;

	private UnstableCheckPoint(Data data, byte[] signature){
		this.data = data;
		this.signature = signature;
	};

	public static UnstableCheckPoint makeUnstableCheckPoint(PrivateKey privateKey, int lastCheckpointNum, int seqNum, String digest){
		Data data = new Data(lastCheckpointNum, digest, seqNum);
		byte[] signature = sign(privateKey, data);
		return new UnstableCheckPoint(data, signature);
	};

	public boolean verify(PublicKey publicKey) {
		return Util.verify(publicKey, this.data, this.signature);
	}

	public boolean isVerified(PublicKey publicKey){
		Boolean checklist[] = new Boolean[2];
		checklist[0] = this.verify(publicKey);
		checklist[1] = (this.getSeqNum() >= this.getLastStableCheckPointNum());
		return Arrays.stream(checklist).allMatch(Boolean::booleanValue);
	}

	public String getDiget(){
		return data.digest;
	}
	public int getLastStableCheckPointNum(){
		return data.lastStableCheckpointNum;
	}
	public int getSeqNum(){
		return data.seqNum;
	}
	private static class Data implements Serializable {
		private final int lastStableCheckpointNum;
		private final int seqNum;
		private final String digest;

		private Data(int lastCheckpointNum, String digest, int seqNum) {
			this.lastStableCheckpointNum = lastCheckpointNum;
			this.digest = digest;
			this.seqNum = seqNum;
		}
	}
}
