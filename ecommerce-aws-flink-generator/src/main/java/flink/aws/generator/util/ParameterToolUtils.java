package flink.aws.generator.util;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ParameterToolUtils {
    public static ParameterTool fromApplicationProperties(Properties properties) {
        Map<String, String> map = new HashMap<>(properties.size());

        properties.forEach((k, v) -> map.put((String) k, (String) v));

        return ParameterTool.fromMap(map);
    }

    public static ParameterTool fromArgsAndApplicationProperties(String[] args) throws IOException {
        //read parameters from command line arguments (for debugging)
        return ParameterTool.fromArgs(args);
    }
}
