package flink.aws.generator;

import flink.aws.generator.input.KafkaProducer;
import flink.aws.generator.input.UserBehaviorCsvFileReader;
import flink.aws.generator.util.Args;
import org.kohsuke.args4j.CmdLineParser;

import java.util.stream.Stream;

public class CSV2KafkaApplication {


    public static void main(String[] args) throws Exception {
        Args arguments = new Args();
        CmdLineParser parser = new CmdLineParser(arguments);
        parser.parseArgument(args);
        // 文件地址
//        String filePath = "/Users/jiasfeng/IdeaProjects/ecommerce-aws-flink/ecommerce-aws-flink-generator/src/main/resources/UserBehavior_random_10000.csv";
        String filePath = arguments.getFile();
        // kafka topic
//        String topic = "user_behavior";
        String topic = arguments.getTopic();
        // kafka borker地址
//        String broker = "localhost:9092";
        String broker = arguments.getBroker();

        Stream.generate(new UserBehaviorCsvFileReader(filePath))
                .sequential()
                .forEachOrdered(new KafkaProducer(topic, broker));
    }
}
