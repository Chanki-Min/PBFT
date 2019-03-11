package kr.ac.hongik.apl;

public class BlockPayload {
    private final String artHash;
    private final String seller;
    private final String buyer;
    private final long price;
    private final long duration;  //as timestamp

    public BlockPayload(String artHash, String seller, String buyer, long price, long duration) {
        this.artHash = artHash;
        this.seller = seller;
        this.buyer = buyer;
        this.price = price;
        this.duration = duration;
    }

    public String getArtHash() {
        return this.artHash;
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

    public long getDuration() {
        return duration;
    }
}
