package org.example.web.analytics;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class AnalyticsRepository {

    private final JdbcTemplate clickHouse;

    public AnalyticsRepository(
            @Qualifier("clickHouseJdbcTemplate") JdbcTemplate clickHouse) {
        this.clickHouse = clickHouse;
    }

    public List<Map<String, Object>> metrics(UUID experimentId, Instant from, Instant to) {
        return clickHouse.query("""
                SELECT
                    toString(variant_id) AS variant_id,
                    uniqExactIf(user_id, event_name = 'exposure' AND attributed = 1) AS exposure_users,
                    uniqExactIf(user_id, event_name = 'click' AND attributed = 1) AS click_users,
                    uniqExactIf(user_id, event_name = 'purchase' AND attributed = 1) AS purchase_users,
                    countIf(event_name = 'purchase' AND attributed = 1) AS orders,
                    sumIf(
                        toDecimal64(JSONExtractFloat(properties, 'amount'), 2),
                        event_name = 'purchase' AND attributed = 1
                    ) AS gmv
                FROM analytics.ab_events FINAL
                WHERE experiment_id = toUUID(?)
                  AND event_time >= parseDateTime64BestEffort(?)
                  AND event_time < parseDateTime64BestEffort(?)
                GROUP BY variant_id
                ORDER BY variant_id
                """, (rs, rowNum) -> {
            long exposures = rs.getLong("exposure_users");
            long clicks = rs.getLong("click_users");
            long purchases = rs.getLong("purchase_users");
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("variant_id", rs.getString("variant_id"));
            row.put("exposure_users", exposures);
            row.put("click_users", clicks);
            row.put("purchase_users", purchases);
            row.put("ctr", rate(clicks, exposures));
            row.put("conversion_rate", rate(purchases, exposures));
            row.put("orders", rs.getLong("orders"));
            row.put("gmv", rs.getBigDecimal("gmv"));
            return row;
        }, experimentId.toString(), from.toString(), to.toString());
    }

    public List<Map<String, Object>> minuteMetrics(UUID experimentId, Instant from, Instant to, int limit) {
        return clickHouse.query("""
                SELECT
                    window_start,
                    window_end,
                    toString(variant_id) AS variant_id,
                    exposures,
                    clicks,
                    purchases,
                    revenue
                FROM analytics.ab_metrics_1m FINAL
                WHERE experiment_id = toUUID(?)
                  AND window_start >= parseDateTime64BestEffort(?)
                  AND window_start < parseDateTime64BestEffort(?)
                ORDER BY window_start DESC, variant_id
                LIMIT ?
                """, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("window_start", rs.getString("window_start"));
            row.put("window_end", rs.getString("window_end"));
            row.put("variant_id", rs.getString("variant_id"));
            row.put("exposures", rs.getLong("exposures"));
            row.put("clicks", rs.getLong("clicks"));
            row.put("purchases", rs.getLong("purchases"));
            row.put("revenue", rs.getBigDecimal("revenue"));
            return row;
        }, experimentId.toString(), from.toString(), to.toString(), limit);
    }

    public List<Map<String, Object>> recentEvents(int limit) {
        return clickHouse.query("""
                SELECT
                    toString(event_id) AS event_id,
                    event_name,
                    event_time,
                    user_id,
                    toString(experiment_id) AS experiment_id,
                    toString(variant_id) AS variant_id,
                    attributed,
                    properties
                FROM analytics.ab_events FINAL
                ORDER BY received_at DESC
                LIMIT ?
                """, (rs, rowNum) -> {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("event_id", rs.getString("event_id"));
            row.put("event_name", rs.getString("event_name"));
            row.put("event_time", rs.getString("event_time"));
            row.put("user_id", rs.getString("user_id"));
            row.put("experiment_id", rs.getString("experiment_id"));
            row.put("variant_id", rs.getString("variant_id"));
            row.put("attributed", rs.getInt("attributed") == 1);
            row.put("properties", rs.getString("properties"));
            return row;
        }, limit);
    }

    private static BigDecimal rate(long numerator, long denominator) {
        if (denominator == 0) {
            return BigDecimal.ZERO;
        }
        return BigDecimal.valueOf(numerator)
                .divide(BigDecimal.valueOf(denominator), 4, RoundingMode.HALF_UP);
    }
}
