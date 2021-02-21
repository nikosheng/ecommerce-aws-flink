package flink.aws.generator.input;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.function.Consumer;

public class KafkaProducer implements Consumer<UserBehavior> {
    private final String topic;
    private final org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]> producer;
    private final JSONSerializer<UserBehavior> serializer;

    public KafkaProducer(String kafkaTopic, String kafkaBrokers) {
        this.topic = kafkaTopic;
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(createKafkaProperties(kafkaBrokers));
        this.serializer = new JSONSerializer<>();
    }

    private static Properties createKafkaProperties(String brokers) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return kafkaProps;
    }

    @Override
    public void accept(UserBehavior userBehavior) {
        // 将对象序列化成byte数组
        byte[] data = serializer.toJSONBytes(userBehavior);
        // 封装
        ProducerRecord<byte[], byte[]> kafkaRecord = new ProducerRecord<>(topic, data);
        // 发送
        producer.send(kafkaRecord);

        // 通过sleep控制消息的速度，请依据自身kafka配置以及flink服务器配置来调整
        try {
            Thread.sleep(500);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
    }
}
