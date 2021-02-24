package flink.aws.hotitems.events;

public class ItemBoard {
    private Integer rank;
    private Long itemid;
    private Long count;
    private Long windowend;

    public ItemBoard(Integer rank, Long itemid, Long count, Long windowend) {
        this.rank = rank;
        this.itemid = itemid;
        this.count = count;
        this.windowend = windowend;
    }

    public Integer getRank() {
        return rank;
    }

    public void setRank(Integer rank) {
        this.rank = rank;
    }

    public Long getItemid() {
        return itemid;
    }

    public void setItemid(Long itemid) {
        this.itemid = itemid;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getWindowend() {
        return windowend;
    }

    public void setWindowend(Long windowend) {
        this.windowend = windowend;
    }
}
