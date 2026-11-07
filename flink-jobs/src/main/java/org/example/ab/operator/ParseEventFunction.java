package org.example.ab.operator;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.ab.TrackingEvent;
import org.example.ab.time.ClickHouseTimes;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/**
 * 把 raw topic 里的 JSON 文本转成 {@link TrackingEvent}。
 * 缺字段、事件名不支持、UUID 或时间格式不对时，不进入后续统计，改走侧输出 {@link #DLQ}。
 */
public class ParseEventFunction extends ProcessFunction<String, TrackingEvent> {

    /** 解析失败的记录。下游直接写到 ab.events.dlq，结构是 raw、error、failed_at。 */
    public static final OutputTag<String> DLQ = new OutputTag<String>("ab-events-dlq") { };

    private static final Set<String> EVENT_NAMES = Set.of("exposure", "click", "purchase");

    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();
    }

    @Override
    public void processElement(String raw, Context context, Collector<TrackingEvent> out) {
        try {
            out.collect(toEvent(mapper.readTree(raw)));
        } catch (Exception ex) {
            context.output(DLQ, invalidRecord(raw, ex));
        }
    }

    private TrackingEvent toEvent(JsonNode node) {
        String eventId = required(node, "event_id");
        String eventName = required(node, "event_name");
        Instant eventInstant = Instant.parse(required(node, "event_time"));
        Instant receivedInstant = Instant.parse(required(node, "received_at"));
        String experimentId = required(node, "experiment_id");
        String variantId = required(node, "variant_id");
        if (!EVENT_NAMES.contains(eventName)) {
            throw new IllegalArgumentException("unsupported event_name");
        }
        UUID.fromString(eventId);
        UUID.fromString(experimentId);
        UUID.fromString(variantId);

        TrackingEvent event = new TrackingEvent();
        event.setEventId(eventId);
        event.setEventName(eventName);
        event.setEventTimestamp(eventInstant.toEpochMilli());
        event.setEventTime(ClickHouseTimes.format(eventInstant));
        event.setReceivedAt(ClickHouseTimes.format(receivedInstant));
        event.setUserId(required(node, "user_id"));
        event.setSessionId(node.path("session_id").asText(""));
        event.setExperimentId(experimentId);
        event.setVariantId(variantId);
        event.setProperties(node.path("properties").toString());
        // ClickHouse ReplacingMergeTree 用这个版本号决定保留哪一行。
        event.setVersion(receivedInstant.toEpochMilli());
        event.setRevenue(readRevenue(node));
        return event;
    }

    /** 购买金额在 properties.amount。没有金额时保持 0，后面聚合直接相加即可。 */
    private static BigDecimal readRevenue(JsonNode node) {
        JsonNode amount = node.path("properties").path("amount");
        if (amount.isNumber() || amount.isTextual()) {
            return new BigDecimal(amount.asText());
        }
        return BigDecimal.ZERO;
    }

    private String invalidRecord(String raw, Exception ex) {
        ObjectNode invalid = mapper.createObjectNode();
        invalid.put("raw", raw);
        invalid.put("error", ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage());
        invalid.put("failed_at", Instant.now().toString());
        return invalid.toString();
    }

    private static String required(JsonNode node, String field) {
        String value = node.path(field).asText("");
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }
}
