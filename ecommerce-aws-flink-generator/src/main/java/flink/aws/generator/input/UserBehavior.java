package flink.aws.generator.input;

import com.fasterxml.jackson.annotation.JsonFormat;

public class UserBehavior {

    @JsonFormat
    private long userid;

    @JsonFormat
    private long itemid;

    @JsonFormat
    private long categoryid;

    @JsonFormat
    private String behavior;

    @JsonFormat
    private Long timestamp;

    public UserBehavior() {
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public long getItemid() {
        return itemid;
    }

    public void setItemid(long itemid) {
        this.itemid = itemid;
    }

    public long getCategoryid() {
        return categoryid;
    }

    public void setCategoryid(long categoryid) {
        this.categoryid = categoryid;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public UserBehavior(long userId, long itemid, long categoryId, String behavior, Long timestamp) {
        this.userid = userId;
        this.itemid = itemid;
        this.categoryid = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }
}
