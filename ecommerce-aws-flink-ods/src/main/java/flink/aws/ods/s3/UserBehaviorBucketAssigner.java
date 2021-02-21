package flink.aws.ods.s3;

import flink.aws.ods.events.UserBehavior;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.joda.time.DateTime;
import org.joda.time.Instant;

import java.io.Serializable;

public class UserBehaviorBucketAssigner implements BucketAssigner<UserBehavior, String>, Serializable {
    private final String prefix;

    public UserBehaviorBucketAssigner(String prefix) {
        this.prefix = prefix;
    }

    public String getBucketId(UserBehavior behavior, Context context) {

        DateTime eventDatetime = Instant
                .ofEpochSecond(behavior.getTimestamp())
                .toDateTime();

        return String.format("%s/year=%04d/month=%02d",
                prefix,
                eventDatetime.getYear(),
                eventDatetime.getMonthOfYear()
        );
    }

    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
