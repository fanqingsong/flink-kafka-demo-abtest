package org.example.ab.time;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * ClickHouse 落库使用的时间文本。Kafka 里的 ISO-8601 在写出前统一成 UTC。
 */
public final class ClickHouseTimes {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private ClickHouseTimes() {
    }

    public static String format(Instant instant) {
        return FORMATTER.format(instant);
    }

    public static String formatEpochMilli(long epochMilli) {
        return format(Instant.ofEpochMilli(epochMilli));
    }
}
