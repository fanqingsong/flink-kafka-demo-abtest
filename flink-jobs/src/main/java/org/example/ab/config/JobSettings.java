package org.example.ab.config;

import org.example.ab.EventAnalyticsJob;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 读取作业配置。默认来自 classpath 上的 {@code config.properties}，
 * 环境变量 {@code BOOTSTRAP_SERVERS} 可以覆盖 Kafka 地址，方便容器里改 broker。
 */
public final class JobSettings {

    private JobSettings() {
    }

    public static Properties load() {
        Properties properties = new Properties();
        try (InputStream input = EventAnalyticsJob.class.getClassLoader()
                .getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new IllegalStateException("config.properties not found");
            }
            properties.load(input);
        } catch (IOException ex) {
            throw new IllegalStateException("cannot load config.properties", ex);
        }
        String bootstrap = System.getenv("BOOTSTRAP_SERVERS");
        if (bootstrap != null && !bootstrap.isBlank()) {
            properties.setProperty("BOOTSTRAP_SERVERS", bootstrap);
        }
        return properties;
    }
}
