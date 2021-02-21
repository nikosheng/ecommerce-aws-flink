package flink.aws.generator.input;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONSerializer<T> {
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public String toJSONString(T r) {
        try {
            return jsonMapper.writeValueAsString(r);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + r, e);
        }
    }

    public byte[] toJSONBytes(T r) {
        try {
            return jsonMapper.writeValueAsBytes(r);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + r, e);
        }
    }
}
