package org.example.ab.operator;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.ab.MetricResult;
import org.example.ab.TrackingEvent;
import org.example.ab.time.ClickHouseTimes;

import java.math.BigDecimal;

/**
 * 把一个 1 分钟窗口里、同一个实验版本的已归因事件收成一行指标。
 * 计数是事件条数，不是去重用户数。购买金额来自解析阶段写入的 revenue。
 */
public class MetricWindowFunction
        extends ProcessWindowFunction<TrackingEvent, MetricResult, String, TimeWindow> {

    @Override
    public void process(
            String key,
            Context context,
            Iterable<TrackingEvent> events,
            Collector<MetricResult> out) {
        MetricResult result = new MetricResult();
        result.setWindowStart(ClickHouseTimes.formatEpochMilli(context.window().getStart()));
        result.setWindowEnd(ClickHouseTimes.formatEpochMilli(context.window().getEnd()));
        result.setVersion(context.window().getEnd());

        BigDecimal revenue = BigDecimal.ZERO;
        for (TrackingEvent event : events) {
            result.setExperimentId(event.getExperimentId());
            result.setVariantId(event.getVariantId());
            if ("exposure".equals(event.getEventName())) {
                result.setExposures(result.getExposures() + 1);
            } else if ("click".equals(event.getEventName())) {
                result.setClicks(result.getClicks() + 1);
            } else if ("purchase".equals(event.getEventName())) {
                result.setPurchases(result.getPurchases() + 1);
                revenue = revenue.add(event.getRevenue());
            }
        }
        result.setRevenue(revenue);
        out.collect(result);
    }
}
