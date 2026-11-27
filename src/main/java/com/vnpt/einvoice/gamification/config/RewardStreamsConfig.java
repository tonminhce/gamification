package com.vnpt.einvoice.gamification.config;

import com.vnpt.einvoice.gamification.metrics.RewardMetrics;
import com.vnpt.einvoice.gamification.streams.RewardTopology;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Configuration
public class RewardStreamsConfig {

    private static final Logger log = LoggerFactory.getLogger(RewardStreamsConfig.class);

    private KafkaStreams managedStreams;

    @Bean
    Topology rewardTopology(
            @Value("${app.kafka.topic}") String sourceTopic,
            @Value("${app.kafka.delta-topic}") String deltaTopic,
            @Value("${app.kafka.dlt-topic}") String dltTopic,
            @Value("${app.dedupe.retention-ms:86400000}") long dedupeRetentionMs,
            @Value("${app.dedupe.bloom-expected-insertions:4000000}") int bloomExpectedInsertions,
            @Value("${app.window.size-ms:500}") long windowSizeMs,
            @Value("${app.window.punctuation-ms:100}") long punctuationMs,
            RewardMetrics metrics
    ) {
        return RewardTopology.build(
                sourceTopic,
                deltaTopic,
                dltTopic,
                dedupeRetentionMs,
                bloomExpectedInsertions,
                windowSizeMs,
                punctuationMs,
                metrics
        );
    }

    @Bean
    KafkaStreams rewardStreams(
            Topology rewardTopology,
            KafkaConnectionDetails connectionDetails,
            @Value("${app.streams.application-id}") String applicationId,
            @Value("${app.streams.threads}") int threads,
            @Value("${app.streams.commit-interval-ms}") long commitIntervalMs,
            @Value("${app.streams.state-dir}") String stateDir,
            @Value("${app.streams.rocksdb-block-cache-mb:256}") long blockCacheMb,
            @Value("${app.streams.rocksdb-write-buffer-mb:128}") long writeBufferMb
    ) {
        List<String> servers = connectionDetails.getStreams().getBootstrapServers();
        if (servers == null || servers.isEmpty()) {
            servers = connectionDetails.getBootstrapServers();
        }
        String bootstrapServers = String.join(",", servers);

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, threads);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, HighThroughputRocksDBConfig.class);
        properties.put(HighThroughputRocksDBConfig.BLOCK_CACHE_MB_CONFIG, blockCacheMb);
        properties.put(HighThroughputRocksDBConfig.WRITE_BUFFER_MB_CONFIG, writeBufferMb);
        properties.put(
                StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class
        );
        properties.put("processing.exception.handler.global.enabled", true);
        properties.put(
                StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                "earliest"
        );
        properties.put(
                StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG),
                "all"
        );
        KafkaStreams streams = new KafkaStreams(rewardTopology, properties);
        streams.setUncaughtExceptionHandler(exception -> {
            log.error("Uncaught exception in Kafka Streams thread, shutting down client", exception);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        this.managedStreams = streams;
        return streams;
    }

    @PreDestroy
    void closeStreams() {
        if (managedStreams != null) {
            managedStreams.close(Duration.ofSeconds(10));
        }
    }

    @Bean
    ApplicationRunner startRewardStreams(KafkaStreams rewardStreams) {
        // Topic beans are created during context refresh; starting here prevents MissingSourceTopicException on fresh brokers.
        return args -> rewardStreams.start();
    }
}
