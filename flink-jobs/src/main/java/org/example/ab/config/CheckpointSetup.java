package org.example.ab.config;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 开启 exactly-once checkpoint。Kafka sink 也按 exactly-once 提交事务，
 * 两边配合后，作业失败重启不会把同一批结果重复写出。
 */
public final class CheckpointSetup {

    private CheckpointSetup() {
    }

    public static void enable(StreamExecutionEnvironment env) {
        env.enableCheckpointing(30_000);
        CheckpointConfig checkpoint = env.getCheckpointConfig();
        checkpoint.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpoint.setMinPauseBetweenCheckpoints(10_000);
        checkpoint.setCheckpointTimeout(60_000);
        checkpoint.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }
}
