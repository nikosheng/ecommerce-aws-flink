package flink.aws.hotitems;

import com.google.gson.Gson;
import flink.aws.hotitems.events.ItemBoard;
import flink.aws.hotitems.events.ItemViewCount;
import flink.aws.hotitems.events.UserBehavior;
import flink.aws.hotitems.events.UserBehaviorSchema;
import flink.aws.hotitems.util.ParameterToolUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

public class StreamingHotItems {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingHotItems.class);

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);

        // set up the streaming execution environment
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<UserBehavior> events;

        events = env
                .addSource(getKafkaSource(parameter))
                .name("Kafka source");

        DataStream<UserBehavior> pvStream = events
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior behavior) {
                        return behavior.getTimestamp();
                    }
                })
                ;

        DataStream<ItemViewCount> windowAggStream = pvStream
                .filter( data -> "pv".contentEquals(data.getBehavior()))
                .keyBy("itemid")
                .timeWindow(Time.hours(1), Time.minutes(1))
                .aggregate(new ItemCountAgg(), new WindowItemCountAgg())
                ;

        DataStream<String> topNstream = windowAggStream
                .keyBy("windowend")
                .process( new TopNHotItems(5) );

        topNstream
                .addSink(getKafkaSink(parameter))
                .name("Kafka sink");
//        topNstream.print();

        env.execute("hotitems analysis");
    }


    private static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        private static final long serialVersionUID = 3424654094661887028L;
        private Integer size;

        public TopNHotItems(Integer size) {
            this.size = size;
        }

        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item_view_count", ItemViewCount.class));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            List<ItemBoard> itemBoardList = Lists.newArrayList();

            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o1.getCount() > o2.getCount() ? -1 : (o1.getCount() == o2.getCount()) ? 0 : 1;
                }
            });

            StringBuilder builder = new StringBuilder();
            builder.append("=================================\n\n");
            builder.append("The end period of window: ").append(new Timestamp(timestamp - 1)).append("\n");

            for ( int i = 0; i < Math.min(size, itemViewCounts.size()); i++) {
                ItemViewCount itemViewCount = itemViewCounts.get(i);
                ItemBoard board = new ItemBoard(
                        i + 1,
                        itemViewCount.getItemid(),
                        itemViewCount.getCount(),
                        itemViewCount.getWindowend()
                );
                itemBoardList.add(board);
                builder.append("No.").append(i + 1).append(": ");
                builder.append("ItemId: ").append(itemViewCount.getItemid()).append("\n");
                builder.append("PageView: ").append(itemViewCount.getCount()).append("\n");
            }

            String jsonItemBoardList = new Gson().toJson(itemBoardList);
            builder.append("=================================\n\n");
            LOG.info(builder.toString());

            Thread.sleep(Time.seconds(1).toMilliseconds());

            out.collect(jsonItemBoardList);
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
            itemViewCountListState.add(itemViewCount);
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowend() + 1);
        }
    }

    private static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBahavior, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    private static class WindowItemCountAgg implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            Long itemId = tuple.getField(0);
            Long windowEnd = timeWindow.getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    private static SourceFunction<UserBehavior> getKafkaSource(ParameterTool parameter) {
        String topic = parameter.getRequired("InputKafkaTopic");
        String bootstrapServers = parameter.getRequired("InputKafkaBootstrapServers");

        LOG.info("Reading from {} Kafka topic", topic);
        LOG.info("Reading from {} Kafka bootstrapServers", bootstrapServers);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id", "kafka-streaming-etl-consumer");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        return new FlinkKafkaConsumer<>(topic, new UserBehaviorSchema(), properties);
    }

    private static SinkFunction<String> getKafkaSink(ParameterTool parameter) {
        String topic = parameter.getRequired("OutputKafkaTopic");
        String bootstrapServers = parameter.getRequired("OutputKafkaBootstrapServers");

        LOG.info("Reading from {} Kafka topic", topic);
        LOG.info("Reading from {} Kafka bootstrapServers", bootstrapServers);

        return new FlinkKafkaProducer<>(bootstrapServers, topic, new SimpleStringSchema());
    }
}
