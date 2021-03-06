package flink.aws.ods;

import com.google.gson.Gson;
import flink.aws.ods.events.UserBehavior;
import flink.aws.ods.events.UserBehaviorSchema;
import flink.aws.ods.s3.UserBehaviorBucketAssigner;
import flink.aws.ods.util.ParameterToolUtils;
import flink.aws.ods.vo.UserBehaviorVO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Properties;

public class StreamingODS {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingODS.class);

    // set up side output stream to output records to S3 and Kafka sink
    private final static OutputTag<UserBehavior> s3OutputTag = new OutputTag<>("s3-output-tag", TypeInformation.of(UserBehavior.class));
    private final static OutputTag<UserBehavior> kafkaOutputTag = new OutputTag<>("kafka-output-tag", TypeInformation.of(UserBehavior.class));

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserBehavior> events;

        events = env
                .addSource(getKafkaSource(parameter))
                .name("Kafka source");

        SingleOutputStreamOperator<UserBehavior> mainStream = events.process(new ProcessFunction<UserBehavior, UserBehavior>() {
            @Override
            public void processElement(UserBehavior userBehavior, Context context, Collector<UserBehavior> collector) throws Exception {
                collector.collect(userBehavior);

                context.output(s3OutputTag, userBehavior);
                context.output(kafkaOutputTag, userBehavior);
            }
        });

        DataStream<UserBehavior> kafkaSinkStream = mainStream.getSideOutput(kafkaOutputTag);

        kafkaSinkStream
                .keyBy(UserBehavior::getItemid)
                .map(new MapFunction<UserBehavior, UserBehaviorVO>() {
                    @Override
                    public UserBehaviorVO map(UserBehavior userBehavior) throws Exception {
                        return new UserBehaviorVO(
                          userBehavior.getUserid(),
                          userBehavior.getItemid(),
                          userBehavior.getCategoryid(),
                          userBehavior.getBehavior().toString(),
                          userBehavior.getTimestamp()
                        );
                    }
                })
                .map(new MapFunction<UserBehaviorVO, String>() {
                    @Override
                    public String map(UserBehaviorVO userBehaviorVO) throws Exception {
                        return new Gson().toJson(userBehaviorVO);
                    }
                })
                .addSink(getKafkaStringSink(parameter))
                .name("Kafka sink");

        DataStream<UserBehavior> s3SinkStream = mainStream.getSideOutput(s3OutputTag);

        s3SinkStream
                .keyBy(UserBehavior::getItemid)
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
        properties.setProperty("group.id", "flink-kafka-ods-1");
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

    private static SinkFunction<String> getKafkaStringSink(ParameterTool parameter) {
        String topic = parameter.getRequired("OutputKafkaTopic");
        String bootstrapServers = parameter.getRequired("OutputKafkaBootstrapServers");

        LOG.info("Reading from {} Kafka topic", topic);
        LOG.info("Reading from {} Kafka bootstrapServers", bootstrapServers);

        return new FlinkKafkaProducer<>(bootstrapServers, topic, new SimpleStringSchema());
    }

    private static SinkFunction<UserBehavior> getS3Sink(ParameterTool parameter) {
        String bucket = parameter.getRequired("OutputBucket");
        LOG.info("Writing to {} bucket", bucket);

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
