package flink.aws.hotitems.events;

import org.apache.avro.data.TimeConversions;
import org.apache.avro.specific.SpecificData;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserBehaviorSchema implements SerializationSchema<UserBehavior>, DeserializationSchema<UserBehavior> {
    private final ObjectMapper mapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(UserBehaviorSchema.class);

    static {
        SpecificData.get().addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
    }

    @Override
    public byte[] serialize(UserBehavior event) {
        return toJson(event).getBytes();
    }

    @Override
    public UserBehavior deserialize(byte[] bytes) {
        try {
            ObjectNode node = this.mapper.readValue(bytes, ObjectNode.class);

            return UserBehavior
                    .newBuilder()
                    .setUserid(node.get("userid").asLong())
                    .setItemid(node.get("itemid").asLong())
                    .setCategoryid(node.get("categoryid").asLong())
                    .setBehavior(node.get("behavior").asText())
                    .setTimestamp(node.get("timestamp").asLong())
                    .build();
        } catch (Exception e) {
            LOG.warn("Failed to serialize event: {}", new String(bytes), e);

            return null;
        }
    }

    @Override
    public boolean isEndOfStream(UserBehavior behavior) {
        return false;
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return new AvroTypeInfo<>(UserBehavior.class);
    }


    public static String toJson(UserBehavior behavior) {
        StringBuilder builder = new StringBuilder();

        builder.append("{");
        addField(builder, behavior, "userid");
        builder.append(", ");
        addField(builder, behavior, "itemid");
        builder.append(", ");
        addField(builder, behavior, "categoryid");
        builder.append(", ");
        addField(builder, behavior, "behavior");
        builder.append(", ");
        addField(builder, behavior, "timestamp");
        builder.append("}");

        return builder.toString();
    }

    private static void addField(StringBuilder builder, UserBehavior behavior, String fieldName) {
        addField(builder, fieldName, behavior.get(fieldName));
    }

    private static void addField(StringBuilder builder, String fieldName, Object value) {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");

        builder.append(": ");
        builder.append(value);
    }

    private static void addTextField(StringBuilder builder, UserBehavior behavior, String fieldName) {
        builder.append("\"");
        builder.append(fieldName);
        builder.append("\"");

        builder.append(": ");
        builder.append("\"");
        builder.append(behavior.get(fieldName));
        builder.append("\"");
    }
}
