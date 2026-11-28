package com.vnpt.einvoice.gamification.config;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic invoiceEventsTopic(
            @Value("${app.kafka.topic}") String topic,
            @Value("${app.kafka.partitions:12}") int partitions,
            @Value("${app.kafka.replication-factor:1}") int replicationFactor
    ) {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }

    @Bean
    NewTopic companyRewardDeltasTopic(
            @Value("${app.kafka.delta-topic}") String topic,
            @Value("${app.kafka.partitions:12}") int partitions,
            @Value("${app.kafka.replication-factor:1}") int replicationFactor
    ) {
        return TopicBuilder.name(topic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }

    @Bean
    NewTopic invoiceEventsDltTopic(
            @Value("${app.kafka.dlt-topic}") String topic,
            @Value("${app.kafka.replication-factor:1}") int replicationFactor
    ) {
        return TopicBuilder.name(topic)
                .partitions(1)
                .replicas(replicationFactor)
                .build();
    }

    @SuppressWarnings("unchecked")
    @Bean
    KafkaTemplate<String, InvoiceIssuedEvent> invoiceKafkaTemplate(
            ProducerFactory<?, ?> producerFactory
    ) {
        return new KafkaTemplate<>((ProducerFactory<String, InvoiceIssuedEvent>) producerFactory);
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        // Keep retrying without acknowledging so a failed Redis update cannot be skipped.
        return new DefaultErrorHandler(new FixedBackOff(1_000L, Long.MAX_VALUE));
    }
}
