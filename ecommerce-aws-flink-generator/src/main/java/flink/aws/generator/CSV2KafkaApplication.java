package flink.aws.generator;

import flink.aws.generator.input.UserBehaviorCsvFileReader;
import flink.aws.generator.input.KafkaProducer;
import flink.aws.generator.util.ParameterToolUtils;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.stream.Stream;

public class CSV2KafkaApplication {
    public static void main(String[] args) throws IOException {
        ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);
        // 文件地址
//        String filePath = "/Users/jiasfeng/IdeaProjects/ecommerce-aws-flink/ecommerce-aws-flink-generator/src/main/resources/UserBehavior_random_10000.csv";
        String filePath = parameter.getRequired("file");
        // kafka topic
//        String topic = "user_behavior";
        String topic = parameter.getRequired("topic");
        // kafka borker地址
//        String broker = "localhost:9092";
        String broker = parameter.getRequired("broker-list");

        Stream.generate(new UserBehaviorCsvFileReader(filePath))
                .sequential()
                .forEachOrdered(new KafkaProducer(topic, broker));
    }
}
