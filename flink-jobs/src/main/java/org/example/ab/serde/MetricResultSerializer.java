package org.example.ab.serde;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.example.ab.MetricResult;

import java.nio.charset.StandardCharsets;

/** 分钟指标写出到 ab.metrics.1m 的 JSON。 */
public class MetricResultSerializer implements SerializationSchema<MetricResult> {

    private transient ObjectMapper mapper;

    @Override
    public byte[] serialize(MetricResult metric) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        ObjectNode node = mapper.createObjectNode();
        node.put("window_start", metric.getWindowStart());
        node.put("window_end", metric.getWindowEnd());
        node.put("experiment_id", metric.getExperimentId());
        node.put("variant_id", metric.getVariantId());
        node.put("exposures", metric.getExposures());
        node.put("clicks", metric.getClicks());
        node.put("purchases", metric.getPurchases());
        node.put("revenue", metric.getRevenue());
        node.put("version", metric.getVersion());
        return node.toString().getBytes(StandardCharsets.UTF_8);
    }
}
