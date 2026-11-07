# 埋点与 A/B 测试 MVP

这条链路演示从分流到指标查询的完整 A/B 测试：

```text
浏览器 SDK -> Spring Boot -> Kafka raw -> Flink -> Kafka valid/metrics -> ClickHouse
                           PostgreSQL 保存实验配置
```

分流、曝光、归因和指标的业务含义见 [A/B 测试业务逻辑](ab-business-logic.md)。
Kafka 和 Flink 在这条链路里各做什么、ClickHouse 如何自己消费 Kafka 落库、字段如何变化，可先看
[数据模型与端到端数据流](data-model-and-flow.md)。

## 启动

```shell
docker compose up --build -d
docker compose ps
```

Compose 会先启动 PostgreSQL、Kafka 和 ClickHouse，再启动 Web 与 Flink 集群。
`event-analytics` 容器等待 Web、ClickHouse 和 Flink 就绪后提交
`Flink AB Event Analytics` 作业，提交完成后退出；作业继续在 TaskManager 中运行。

入口：

- A/B 演示：<http://localhost:8088/ab-demo.html>（首页 <http://localhost:8088/> 相同）
- 实验 Dashboard：<http://localhost:8088/ab-dashboard.html>
- 分钟指标：<http://localhost:8088/ab-metrics-1m.html>
- Flink UI：<http://localhost:8081/>

数据库：

- PostgreSQL：`localhost:5432/ab_testing`
- ClickHouse HTTP：`localhost:8123`
- ClickHouse Native：`localhost:9000`

首次启动时 Flyway 会创建实验表并写入 `checkout-button` 演示实验。
该实验 `traffic_percent=100`，即全部用户进入实验；两个版本的 `weight` 各为 50。

## JS SDK

```html
<script src="http://localhost:8088/sdk/ab-sdk.js"></script>
<script>
  AbSdk.init({ apiBase: "http://localhost:8088" });
  const assignment = await AbSdk.getVariant("checkout-button");

  if (assignment.in_experiment) {
    button.textContent = assignment.payload.button_text;
    await AbSdk.track("click", { target: "buy_button" }, "checkout-button");
    await AbSdk.track("purchase", { amount: 99 }, "checkout-button");
  }
</script>
```

`getVariant` 会为当前会话自动发送一次 `exposure`。匿名用户 ID 保存在
`localStorage`，会话 ID 保存在 `sessionStorage`。

## HTTP API

稳定分流：

```shell
curl -X POST http://localhost:8088/api/assignments \
  -H 'Content-Type: application/json' \
  -d '{"experiment_key":"checkout-button","user_id":"user-001"}'
```

单条埋点：

```shell
curl -X POST http://localhost:8088/api/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id":"12345678-1234-4234-8234-123456789012",
    "event_name":"exposure",
    "event_time":"2026-11-05T04:30:00Z",
    "user_id":"user-001",
    "session_id":"session-001",
    "experiment_id":"11111111-1111-1111-1111-111111111111",
    "variant_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "properties":{}
  }'
```

`POST /api/events` 也接受最多 100 条事件组成的 JSON 数组。
服务端会校验实验正在运行且版本属于该实验，并补充 `received_at` 后写入
`ab.events.raw`。如果绕过 API 直接向 raw topic 写数据，也必须提供 ISO-8601 格式的
`received_at`，否则事件会进入 DLQ。

## 数据口径

- 分流：`SHA-256(experimentKey:userId:salt)`，相同配置下同一用户版本稳定。
- 曝光：同一用户和实验在 7 天归因窗口内只计第一次曝光。
- 点击/购买：必须在最近曝光后 7 天内，且版本相同，才标记为已归因。
- 去重：Flink 按 `event_id` 保留 24 小时状态。
- 事件时间：允许 30 秒乱序；分钟指标使用事件时间滚动窗口。
- CTR：已归因点击 UV / 曝光 UV。
- 转化率：已归因购买 UV / 曝光 UV。
- 订单数：已归因购买事件条数。
- GMV：已归因购买事件 `properties.amount` 之和。

这里有两套不同粒度的指标：

- `analytics.ab_metrics_1m` 中的 `exposures`、`clicks`、`purchases` 是一分钟窗口内的**事件条数**。分钟指标页读取这张表。
- Dashboard API 不读取分钟表，而是从 `analytics.ab_events FINAL` 计算曝光、点击和购买**用户数**，再计算 CTR 与转化率。

因此分钟表中的 `clicks` 不能直接与 Dashboard 的“点击用户数”比较。A/B Flink
作业每 30 秒 checkpoint，写 Kafka 的 valid、DLQ 和 metrics sink 使用
`EXACTLY_ONCE`。

## Topic

```text
ab.events.raw
ab.events.valid
ab.events.dlq
ab.metrics.1m
```

四个 A/B topic 均为 4 个分区。`ab.events.raw` 使用 `user_id` 作为 Kafka key，
保证同一用户的事件进入同一分区；Flink 后续仍会按处理键重新分区。

查看消息：

```shell
docker compose exec kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ab.events.valid --from-beginning
```

查询 ClickHouse：

```shell
docker compose exec clickhouse clickhouse-client --query \
  "SELECT event_name, variant_id, count() FROM analytics.ab_events FINAL GROUP BY event_name, variant_id"
```

## 常见排查

1. Dashboard 没有数据：先确认 Flink UI 中 `Flink AB Event Analytics` 为 RUNNING。
2. 事件进入 DLQ：消费 `ab.events.dlq` 查看 `error` 字段。
3. ClickHouse 暂时无数据：检查 `system.kafka_consumers` 和 Kafka Engine 的消费异常。
4. 修改 Flink 代码后：先在 Flink UI 取消旧作业，再执行
   `docker compose up --build -d event-analytics`。
5. 完全清空演示数据：执行 `docker compose down -v` 后重新启动。
