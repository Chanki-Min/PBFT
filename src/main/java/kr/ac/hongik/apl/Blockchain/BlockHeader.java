package kr.ac.hongik.apl.Blockchain;

public class BlockHeader {
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
		return String.format("(%d,%s,%s,%b)", blockNumber, rootHash ,prevHash, hasHashList);
	}

	public Integer getBlockNumber() {return blockNumber;}
	public String getRootHash() {return rootHash;}
	public String getPrevHash() {return prevHash;}
	public Boolean getHasHashList() {return hasHashList;}
}
