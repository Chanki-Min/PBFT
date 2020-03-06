package kr.ac.hongik.apl.Messages;

import kr.ac.hongik.apl.Blockchain.BlockHeader;
import kr.ac.hongik.apl.Util;

import java.io.Serializable;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;

public class GossipReplyMessage implements Message {
	private Data data;
	private byte[] signature;

	public GossipReplyMessage(Data data, byte[] signature) {
		this.data = data;
		this.signature = signature;
	}

	public static GossipReplyMessage makeGossipReplyMessage(PrivateKey privateKey, int myNumber, Map<String, BlockHeader> latestHeaders, PublicKey clientPublicKey, long requestEpochTime) {
		Data data = new Data(latestHeaders, myNumber, clientPublicKey, requestEpochTime);
		byte[] signature = Util.sign(privateKey, data);

		return new GossipReplyMessage(data, signature);
	}

	public boolean verify(PublicKey publicKey) {
		return Util.verify(publicKey, this.data, this.signature);
	}

	@Override
	public boolean equals(Object object) {
		if (!(object instanceof GossipReplyMessage)) return false;

		return this.data.latestHeaders.equals(((GossipReplyMessage) object).data.latestHeaders);
	}

	public Map<String, BlockHeader> getLatestHeaders() {
		return this.data.latestHeaders;
	}

	public PublicKey getClientPublicKey() {
		return this.data.clientPublicKey;
	}

	public long getRequestEpochTime() {
		return this.data.requestEpochTime;
	}

	public int getReplicaNumber() {
		return this.data.replicaNumber;
	}

	private static class Data implements Serializable {
		private Map<String, BlockHeader> latestHeaders;
		private int replicaNumber;
		private PublicKey clientPublicKey;
		private long requestEpochTime;

		public Data(Map<String, BlockHeader> latestHeaders, int replicaNumber, PublicKey clientPublicKey, long requestEpochTime) {
			this.latestHeaders = latestHeaders;
			this.replicaNumber = replicaNumber;
			this.clientPublicKey = clientPublicKey;
			this.requestEpochTime = requestEpochTime;
		}
	}
}
