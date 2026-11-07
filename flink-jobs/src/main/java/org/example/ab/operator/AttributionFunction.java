package org.example.ab.operator;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.ab.TrackingEvent;
import org.example.ab.attribution.AttributionRules;
import org.example.ab.attribution.ExposureState;

/**
 * 给每条事件打上 {@code attributed}。调用前按 {@code user_id + experiment_id} 分区，
 * 这样同一个用户在同一个实验上的曝光和行为会进同一个状态。
 *
 * 曝光会刷新“最近一次曝光”。click / purchase 只和这份状态比较，不改它。
 * 状态比归因窗口多留 1 天，避免窗口边缘的状态先被清掉。
 */
public class AttributionFunction extends KeyedProcessFunction<String, TrackingEvent, TrackingEvent> {

    private final long attributionWindowMillis;
    private transient ValueState<ExposureState> exposure;

    public AttributionFunction(long attributionWindowMillis) {
        this.attributionWindowMillis = attributionWindowMillis;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<ExposureState> descriptor =
                new ValueStateDescriptor<>("latest-exposure", ExposureState.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(8)).build());
        exposure = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(TrackingEvent event, Context context, Collector<TrackingEvent> out)
            throws Exception {
        ExposureState current = exposure.value();
        if ("exposure".equals(event.getEventName())) {
            boolean firstInWindow = AttributionRules.isFirstExposure(
                    current, event.getEventTimestamp(), attributionWindowMillis);
            event.setAttributed(firstInWindow ? 1 : 0);
            exposure.update(new ExposureState(event.getEventTimestamp(), event.getVariantId()));
        } else {
            boolean inWindow = AttributionRules.isWithinAttribution(
                    current,
                    event.getVariantId(),
                    event.getEventTimestamp(),
                    attributionWindowMillis
            );
            event.setAttributed(inWindow ? 1 : 0);
        }
        out.collect(event);
    }
}
