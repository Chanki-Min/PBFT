package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Util;

import java.nio.channels.SocketChannel;
import java.security.PrivateKey;
import java.security.PublicKey;

public class GossipRequestMessage implements Message {
	public int replicaNum;
	public PublicKey clientPublicKey;
	public long requestTime;
	public byte[] signature;
	public SocketChannel socketChannel;

	public GossipRequestMessage(int replicaNum, PublicKey clientPublicKey, long requestTime, byte[] signature) {
		this.replicaNum = replicaNum;
		this.clientPublicKey = clientPublicKey;
		this.requestTime = requestTime;
		this.signature = signature;

	}

	public static GossipRequestMessage makeGossipRequestMessage(PrivateKey privateKey, int replicaNum, PublicKey clientPublicKey, long requestTime) {
		byte[] signature = Util.sign(privateKey, replicaNum);
		return new GossipRequestMessage(replicaNum, clientPublicKey, requestTime, signature);
	}

	public boolean verify(PublicKey publicKey) {
		return Util.verify(publicKey, replicaNum, signature);
	}


}
