package org.example.ab.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Properties;

/**
 * 作业用到的 Kafka 读写。输入是原始埋点，输出分别是有效事件、坏数据和分钟指标。
 */
public final class KafkaIO {

    private KafkaIO() {
    }

    /** 从最早位点消费 raw topic，保证作业第一次启动时能补齐历史埋点。 */
    public static KafkaSource<String> rawEvents(Properties properties) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("BOOTSTRAP_SERVERS"))
                .setTopics(properties.getProperty("AB_EVENTS_RAW_TOPIC"))
                .setGroupId("flink-ab-event-analytics-v1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    /**
     * 事务写出。{@code transactionPrefix} 在三个 sink 之间必须不同，
     * 否则它们会抢同一组 Kafka 事务 id。
     */
    public static <T> KafkaSink<T> sink(
            Properties properties,
            String topic,
            String transactionPrefix,
            SerializationSchema<T> serializer) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(properties.getProperty("BOOTSTRAP_SERVERS"))
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(serializer)
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transactionPrefix)
                .build();
    }
}
