package org.example.ab.serde;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.example.ab.TrackingEvent;

import java.nio.charset.StandardCharsets;

/** 有效事件写出到 ab.events.valid 的 JSON。一行一条，字段名和 ClickHouse Kafka 表一致。 */
public class TrackingEventSerializer implements SerializationSchema<TrackingEvent> {

    private transient ObjectMapper mapper;

    @Override
    public byte[] serialize(TrackingEvent event) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        ObjectNode node = mapper.createObjectNode();
        node.put("event_id", event.getEventId());
        node.put("event_name", event.getEventName());
        node.put("event_time", event.getEventTime());
        node.put("received_at", event.getReceivedAt());
        node.put("user_id", event.getUserId());
        node.put("session_id", event.getSessionId());
        node.put("experiment_id", event.getExperimentId());
        node.put("variant_id", event.getVariantId());
        node.put("properties", event.getProperties());
        node.put("attributed", event.getAttributed());
        node.put("version", event.getVersion());
        return node.toString().getBytes(StandardCharsets.UTF_8);
    }
}
