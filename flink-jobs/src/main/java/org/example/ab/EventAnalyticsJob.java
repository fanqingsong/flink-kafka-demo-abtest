package org.example.ab;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.ab.config.CheckpointSetup;
import org.example.ab.config.JobSettings;
import org.example.ab.kafka.KafkaIO;
import org.example.ab.operator.AttributionFunction;
import org.example.ab.operator.DeduplicateFunction;
import org.example.ab.operator.MetricWindowFunction;
import org.example.ab.operator.ParseEventFunction;
import org.example.ab.serde.MetricResultSerializer;
import org.example.ab.serde.TrackingEventSerializer;

import java.time.Duration;
import java.util.Properties;

/**
 * A/B 埋点实时作业。这个类只把算子串起来，每一步的规则在对应类里。
 *
 * <p>下面用同一个人、同一个实验走完一条链路。用户 {@code u1}，实验 {@code exp}，版本 {@code A} 和 {@code B}。
 *
 * <pre>
 *  ab.events.raw
 *        │
 *        ▼
 *     解析 JSON
 *        ├─ 失败 ─────────────────────────────► ab.events.dlq
 *        ▼ 成功
 *     用 event_time 标水位（允许晚 30 秒）
 *        ▼
 *     按 event_id 去重（24 小时内只留第一条）
 *        ▼
 *     按 u1|exp 归因，打上 attributed
 *        ├─ 每条都写，含 attributed = 0 ─────► ab.events.valid
 *        ▼ 只留 attributed = 1
 *     按 exp|A、exp|B 切成 1 分钟
 *        └──────────────────────────────────► ab.metrics.1m
 * </pre>
 *
 * 到达顺序是这 6 条：
 *
 * <pre>
 *  #   event_id   name      event_time   variant   amount   这条是什么
 *  1   e1         exposure  10:00:00     A         -        第一次看到版本 A
 *  2   e1         exposure  10:00:00     A         -        同一条又上报了一次
 *  3   {坏 JSON}                                      缺 event_id
 *  4   e2         click     10:00:40     A         -        曝光后 40 秒，仍是版本 A
 *  5   e3         click     10:00:50     B         -        点的是另一个版本
 *  6   e4         purchase  10:01:10     A         19.9     下一分钟的购买
 * </pre>
 *
 * 三条输出里实际留下：
 *
 * <pre>
 *  ab.events.dlq
 *        #3    raw 原文 + error + failed_at
 *
 *  ab.events.valid
 *        #1    attributed = 1     窗口内第一次曝光
 *        #4    attributed = 1     曝光之后、同一版本、7 天以内
 *        #5    attributed = 0     版本不是这次曝光的 A
 *        #6    attributed = 1     同一版本上的购买
 *        #2 被去重丢掉，这里没有它
 *
 *  ab.metrics.1m          只计 attributed = 1，按 event_time 落进哪一分钟
 *        [10:00, 10:01)  版本 A    exposure = 1    click = 1
 *        [10:01, 10:02)  版本 A    purchase = 1    revenue = 19.9
 *        版本 B 没有已归因事件，所以没有这一行
 * </pre>
 *
 * 水位只决定分钟窗口收不收这条事件，valid 照常写。先处理一条 {@code event_time = 10:01:00} 的事件后，
 * 水位停在 {@code 10:00:30}。之后才到的 {@code 10:00:40} 仍进入 {@code [10:00, 10:01)}；
 * 之后才到的 {@code 10:00:20} 已经晚于水位，不会出现在任何分钟桶里。
 */
public class EventAnalyticsJob {

    /** 曝光之后，同一版本上的点击和购买在这个时间内才算归因成功。 */
    private static final Duration ATTRIBUTION_WINDOW = Duration.ofDays(7);

    /** 这个时间内到达的乱序事件仍计入当前窗口。 */
    private static final Duration OUT_OF_ORDERNESS = Duration.ofSeconds(30);
    
    /** 分区这么久没有新数据就标成空闲，避免它拖住其它分区的水位。 */
    private static final Duration SOURCE_IDLE_TIMEOUT = Duration.ofMinutes(1);

    public static void main(String[] args) throws Exception {
        Properties properties = JobSettings.load();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointSetup.enable(env);

        SingleOutputStreamOperator<TrackingEvent> parsed = parseRawEvents(env, properties);
        writeInvalidEvents(parsed, properties);

        SingleOutputStreamOperator<TrackingEvent> attributed = attribute(deduplicate(assignEventTime(parsed)));
        writeValidEvents(attributed, properties);
        writeMinuteMetrics(attributed, properties);

        env.execute("Flink AB Event Analytics");
    }

    /**
     * 读 {@code ab.events.raw}，把 JSON 转成事件。
     *
     * <pre>
     *  #1 #4 #5 #6  → 主流，继续去重和归因
     *  #3 坏 JSON   → 侧输出，交给 writeInvalidEvents
     * </pre>
     */
    private static SingleOutputStreamOperator<TrackingEvent> parseRawEvents(
            StreamExecutionEnvironment env, Properties properties) {
        return env.fromSource(
                        KafkaIO.rawEvents(properties),
                        WatermarkStrategy.noWatermarks(),
                        "AB Raw Events")
                .process(new ParseEventFunction());
    }

    /**
     * 窗口时间用客户端的 {@code event_time}，不用这条消息到达 Flink 的时间。
     *
     * <pre>
     *  先到 event_time = 10:01:00，水位推到 10:00:30
     *  后到 10:00:40  → 还在 30 秒内，进入 [10:00, 10:01)
     *  后到 10:00:20  → 水位已过，分钟指标不要它
     *
     *  某个分区 1 分钟没数据就标成空闲。
     *  否则这个空分区会把别的分区的分钟窗口一直卡住。
     * </pre>
     */
    private static SingleOutputStreamOperator<TrackingEvent> assignEventTime(DataStream<TrackingEvent> events) {
        return events.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrackingEvent>forBoundedOutOfOrderness(OUT_OF_ORDERNESS)
                        .withTimestampAssigner((event, previous) -> event.getEventTimestamp())
                        .withIdleness(SOURCE_IDLE_TIMEOUT));
    }

    /**
     * 同一个 {@code event_id} 在 24 小时内只放行第一条。
     *
     * <pre>
     *  #1 e1  exposure  → 留下
     *  #2 e1  exposure  → 丢掉，后面的归因和分钟指标都看不到它
     * </pre>
     */
    private static SingleOutputStreamOperator<TrackingEvent> deduplicate(DataStream<TrackingEvent> events) {
        return events.keyBy(TrackingEvent::getEventId)
                .process(new DeduplicateFunction());
    }

    /**
     * 按 {@code user_id|experiment_id} 记住最近一次曝光，再给每条事件打 {@code attributed}。
     * {@code u1} 在另一个实验上的曝光不会混进来。
     *
     * <pre>
     *  #1 exposure A   7 天内第一次曝光     attributed = 1，记住「10:00:00 的版本 A」
     *  #4 click    A   曝光后 40 秒         attributed = 1
     *  #5 click    B   版本对不上           attributed = 0
     *  #6 purchase A   19.9，仍在 7 天内    attributed = 1
     *
     *  若 10:05 又来一次版本 A 的曝光：attributed = 0，
     *  但「最近曝光」改成 10:05。这之后的点击改和 10:05 比。
     * </pre>
     */
    private static SingleOutputStreamOperator<TrackingEvent> attribute(DataStream<TrackingEvent> events) {
        return events.keyBy(event -> event.getUserId() + "|" + event.getExperimentId())
                .process(new AttributionFunction(ATTRIBUTION_WINDOW.toMillis()));
    }

    /**
     * 去重、归因之后的事件全部写入 {@code ab.events.valid}，包含没归因上的。
     *
     * <pre>
     *  写入 #1 #4 #5 #6
     *  #5 的 attributed = 0 也在里面，方便核对「为什么没进分钟指标」
     * </pre>
     */
    private static void writeValidEvents(DataStream<TrackingEvent> events, Properties properties) {
        write(events, properties, "AB_EVENTS_VALID_TOPIC", "ab-valid-",
                new TrackingEventSerializer(), "Valid Events");
    }

    /**
     * 解析失败的原文写入 {@code ab.events.dlq}，不参与去重、归因和分钟指标。
     *
     * <pre>
     *  #3 → {"raw":"...","error":"event_id is required","failed_at":"..."}
     * </pre>
     */
    private static void writeInvalidEvents(
            SingleOutputStreamOperator<TrackingEvent> parsed, Properties properties) {
        write(parsed.getSideOutput(ParseEventFunction.DLQ), properties, "AB_EVENTS_DLQ_TOPIC", "ab-dlq-",
                new SimpleStringSchema(), "Invalid Events");
    }

    /**
     * 只统计 {@code attributed = 1} 的事件。同一个实验版本、同一分钟收成一行。
     * 计数是事件条数，不是去重后的人数。
     *
     * <pre>
     *  #5 attributed = 0，被滤掉
     *
     *  [10:00, 10:01)  exp|A   #1 曝光 + #4 点击
     *  [10:01, 10:02)  exp|A   #6 购买，revenue = 19.9
     * </pre>
     */
    private static void writeMinuteMetrics(DataStream<TrackingEvent> attributed, Properties properties) {
        DataStream<MetricResult> metrics = attributed
                .filter(EventAnalyticsJob::isAttributed)
                .keyBy(event -> event.getExperimentId() + "|" + event.getVariantId())
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new MetricWindowFunction());
        write(metrics, properties, "AB_METRICS_TOPIC", "ab-metrics-",
                new MetricResultSerializer(), "Minute Metrics");
    }

    private static boolean isAttributed(TrackingEvent event) {
        return event.getAttributed() == 1;
    }

    private static <T> void write(
            DataStream<T> stream,
            Properties properties,
            String topicProperty,
            String transactionPrefix,
            SerializationSchema<T> serializer,
            String name) {
        stream.sinkTo(KafkaIO.sink(
                properties,
                properties.getProperty(topicProperty),
                transactionPrefix,
                serializer
        )).name(name);
    }
}
