package flink.aws.ods;

import flink.aws.ods.events.UserBehavior;
import flink.aws.ods.events.UserBehaviorSchema;
import flink.aws.ods.util.ParameterToolUtils;
import flink.aws.ods.s3.UserBehaviorBucketAssigner;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Properties;

public class StreamingODS {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingODS.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserBehavior> events;

        events = env
                .addSource(getKafkaSource(parameter))
                .name("Kafka source");

        events
                .keyBy(UserBehavior::getItemId)
                .addSink(getKafkaSink(parameter))
                .name("Kafka sink");

        events
                .keyBy(UserBehavior::getItemId)
                .addSink(getS3Sink(parameter))
                .name("S3 sink");

        env.execute();
    }

    public static DataStream<UserBehavior> createKafkaSource(StreamExecutionEnvironment env, ParameterTool parameter) {

        String topic = parameter.getRequired("InputKafkaTopic");
        String bootstrapServers = parameter.getRequired("InputKafkaBootstrapServers");

        LOG.info("Reading from {} Kafka topic", topic);
        LOG.info("Reading from {} Kafka bootstrapServers", bootstrapServers);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("group.id", "kafka-streaming-etl-consumer");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        //create Kafka source

        return env.addSource(new FlinkKafkaConsumer<>(
                topic,
                //deserialize events with EventSchema
                new UserBehaviorSchema(),
                //using the previously defined properties
                properties
        )).name("KafkaSource");
    }


    private static SourceFunction<UserBehavior> getKafkaSource(ParameterTool parameter) {
        String topic = parameter.getRequired("InputKafkaTopic");
        String bootstrapServers = parameter.getRequired("InputKafkaBootstrapServers");

        LOG.info("Reading from {} Kafka topic", topic);
        LOG.info("Reading from {} Kafka bootstrapServers", bootstrapServers);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("group.id", "kafka-streaming-etl-consumer");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new FlinkKafkaConsumer<>(topic, new UserBehaviorSchema(), properties);
    }

    private static SinkFunction<UserBehavior> getKafkaSink(ParameterTool parameter) {
        String topic = parameter.getRequired("OutputKafkaTopic");
        String bootstrapServers = parameter.getRequired("OutputKafkaBootstrapServers");

        LOG.info("Reading from {} Kafka topic", topic);
        LOG.info("Reading from {} Kafka bootstrapServers", bootstrapServers);

        return new FlinkKafkaProducer<>(bootstrapServers, topic, new UserBehaviorSchema());
    }

    private static SinkFunction<UserBehavior> getS3Sink(ParameterTool parameter) {
        String bucket = parameter.getRequired("OutputBucket");
        LOG.info("Writing to {} buket", bucket);

        String prefix = String.format("%sjob_start=%s/", parameter.get("OutputPrefix", ""), System.currentTimeMillis());

        if (parameter.getBoolean("ParquetConversion", false)) {
            return StreamingFileSink
                    .forBulkFormat(
                            new Path(bucket),
                            ParquetAvroWriters.forSpecificRecord(UserBehavior.class)
                    )
                    .withBucketAssigner(new UserBehaviorBucketAssigner(prefix))
                    .build();
        } else {
            return StreamingFileSink
                    .forRowFormat(
                            new Path(bucket),
                            (Encoder<UserBehavior>) (element, outputStream) -> {
                                PrintStream out = new PrintStream(outputStream);
                                out.println(UserBehaviorSchema.toJson(element));
                            }
                    )
                    .withBucketAssigner(new UserBehaviorBucketAssigner(prefix))
                    .withRollingPolicy(DefaultRollingPolicy.create().build())
                    .build();
        }
    }

}
