package kr.ac.hongik.apl.Blockchain;

import com.fasterxml.jackson.core.JsonProcessingException;
import kr.ac.hongik.apl.Util;

import java.io.Serializable;

public class BlockHeader implements Serializable {
	private Integer blockNumber;
	private String rootHash;
	private String prevHash;
	private Boolean hasHashList;

	public BlockHeader(Integer blockNumber, String rootHash, String prevHash, Boolean hasHashList) {
		this.blockNumber = blockNumber;
		this.rootHash = rootHash;
		this.prevHash = prevHash;
		this.hasHashList = hasHashList;
	}

	public String toString() {
		try {
			return Util.objectMapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	public Integer getBlockNumber() {return blockNumber;}
	public String getRootHash() {return rootHash;}
	public String getPrevHash() {return prevHash;}
	public Boolean getHasHashList() {return hasHashList;}
}
