package org.example.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.topics")
public record TopicNames(
        String eventsRaw,
        String eventsValid,
        String eventsDlq,
        String metrics
) {
}
