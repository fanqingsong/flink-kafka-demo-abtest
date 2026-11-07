package org.example.ab.operator;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.ab.TrackingEvent;

/**
 * 按 {@code event_id} 去掉重复上报。调用前先 {@code keyBy(TrackingEvent::getEventId)}。
 * 同一个 id 在 24 小时内只放行第一条；TTL 到期后状态消失，相同 id 会再次进入后续统计。
 */
public class DeduplicateFunction extends KeyedProcessFunction<String, TrackingEvent, TrackingEvent> {

    private transient ValueState<Boolean> seen;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("seen-event-id", Boolean.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(24)).build());
        seen = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(TrackingEvent event, Context context, Collector<TrackingEvent> out)
            throws Exception {
        if (!Boolean.TRUE.equals(seen.value())) {
            seen.update(Boolean.TRUE);
            out.collect(event);
        }
    }
}
