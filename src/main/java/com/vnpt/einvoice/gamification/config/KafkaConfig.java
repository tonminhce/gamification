package com.vnpt.einvoice.gamification.config;

import com.vnpt.einvoice.gamification.event.InvoiceIssuedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    org.apache.kafka.clients.admin.NewTopic invoiceEventsTopic(
            @Value("${app.kafka.topic}") String topic
    ) {
        return TopicBuilder.name(topic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    ProducerFactory<String, InvoiceIssuedEvent> invoiceProducerFactory(KafkaProperties properties) {
        return new DefaultKafkaProducerFactory<>(properties.buildProducerProperties());
    }

    @Bean
    KafkaTemplate<String, InvoiceIssuedEvent> invoiceKafkaTemplate(
            ProducerFactory<String, InvoiceIssuedEvent> producerFactory
    ) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        // Keep retrying without acknowledging so a failed update cannot be skipped.
        return new DefaultErrorHandler(new FixedBackOff(1_000L, Long.MAX_VALUE));
    }
}
