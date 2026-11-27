package com.vnpt.einvoice.gamification.config;

import com.vnpt.einvoice.gamification.streams.RewardTopology;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class RewardStreamsConfig {

    @Bean
    Topology rewardTopology(
            @Value("${app.kafka.topic}") String sourceTopic,
            @Value("${app.kafka.delta-topic}") String deltaTopic,
            @Value("${app.kafka.dlt-topic}") String dltTopic
    ) {
        return RewardTopology.build(sourceTopic, deltaTopic, dltTopic);
    }

    @Bean(initMethod = "start", destroyMethod = "close")
    KafkaStreams rewardStreams(
            Topology rewardTopology,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${app.streams.application-id}") String applicationId,
            @Value("${app.streams.threads}") int threads,
            @Value("${app.streams.state-dir}") String stateDir
    ) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, threads);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(
                StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                "earliest"
        );
        properties.put(
                StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG),
                "all"
        );
        return new KafkaStreams(rewardTopology, properties);
    }
}