package kr.ac.hongik.apl;

public class BlockPayload implements Message {
    private final String artHash;
    private final String seller;
    private final String buyer;
    private final long price;
    private final Long duration;  //as timestamp

    public BlockPayload(String artHash, String seller, String buyer, long price, Long duration) {
        this.artHash = artHash;
        this.seller = seller;
        this.buyer = buyer;
        this.price = price;
        this.duration = duration;
    }


    public String getArtHash() {
        return this.artHash;
    }


	@Override
    public String toString() {
        return artHash + seller + buyer + price + duration;
    }

    public String getSeller() {
        return seller;
    }

    public String getBuyer() {
        return buyer;
    }

    public long getPrice() {
        return price;
    }

    public Long getDuration() {
        return duration;
    }


}
