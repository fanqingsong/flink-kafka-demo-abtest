package org.example.web;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaSetup {

    @Bean
    public NewTopic eventsRawTopic(TopicNames topics) {
        return eventTopic(topics.eventsRaw());
    }

    @Bean
    public NewTopic eventsValidTopic(TopicNames topics) {
        return eventTopic(topics.eventsValid());
    }

    @Bean
    public NewTopic eventsDlqTopic(TopicNames topics) {
        return eventTopic(topics.eventsDlq());
    }

    @Bean
    public NewTopic metricsTopic(TopicNames topics) {
        return eventTopic(topics.metrics());
    }

    private static NewTopic eventTopic(String name) {
        return TopicBuilder.name(name).partitions(4).replicas(1).build();
    }
}
