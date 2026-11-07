package org.example.web.analytics;

import org.example.web.ApiException;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
public class AnalyticsController {

    private final AnalyticsRepository analytics;

    public AnalyticsController(AnalyticsRepository analytics) {
        this.analytics = analytics;
    }

    @GetMapping("/api/experiments/{id}/metrics")
    public List<Map<String, Object>> metrics(
            @PathVariable("id") UUID id,
            @RequestParam(name = "from", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(name = "to", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {
        Instant actualTo = to == null ? Instant.now() : to;
        Instant actualFrom = from == null ? actualTo.minus(24, ChronoUnit.HOURS) : from;
        if (!actualFrom.isBefore(actualTo)) {
            throw new ApiException(400, "from 必须早于 to");
        }
        return analytics.metrics(id, actualFrom, actualTo);
    }

    @GetMapping("/api/experiments/{id}/metrics/1m")
    public List<Map<String, Object>> minuteMetrics(
            @PathVariable("id") UUID id,
            @RequestParam(name = "from", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(name = "to", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(name = "limit", defaultValue = "500") int limit) {
        Instant actualTo = to == null ? Instant.now() : to;
        Instant actualFrom = from == null ? actualTo.minus(60, ChronoUnit.MINUTES) : from;
        if (!actualFrom.isBefore(actualTo)) {
            throw new ApiException(400, "from 必须早于 to");
        }
        if (limit < 1 || limit > 2000) {
            throw new ApiException(400, "limit 必须在 1 到 2000 之间");
        }
        return analytics.minuteMetrics(id, actualFrom, actualTo, limit);
    }

    @GetMapping("/api/events/recent")
    public List<Map<String, Object>> recent(
            @RequestParam(name = "limit", defaultValue = "50") int limit) {
        if (limit < 1 || limit > 200) {
            throw new ApiException(400, "limit 必须在 1 到 200 之间");
        }
        return analytics.recentEvents(limit);
    }
}
