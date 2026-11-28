package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class LeaderboardProjector {

    private static final Logger log = LoggerFactory.getLogger(LeaderboardProjector.class);

    private final LeaderboardApplyService applyService;

    public LeaderboardProjector(LeaderboardApplyService applyService) {
        this.applyService = applyService;
    }

    @KafkaListener(id = "leaderboardProjector", idIsGroup = false, topics = "${app.kafka.delta-topic}", batch = "true")
    public void consume(List<ConsumerRecord<String, CompanyRewardDelta>> records) {
        if (records.isEmpty()) {
            return;
        }

        ConsumerRecord<String, CompanyRewardDelta> firstValid = null;
        boolean singlePartition = true;
        boolean allValid = true;

        for (ConsumerRecord<String, CompanyRewardDelta> record : records) {
            if (!valid(record.value())) {
                allValid = false;
                log.warn(
                        "Skipping invalid company reward delta at topic={} partition={} offset={}",
                        record.topic(),
                        record.partition(),
                        record.offset()
                );
                continue;
            }
            if (firstValid == null) {
                firstValid = record;
            } else if (singlePartition
                    && (record.partition() != firstValid.partition()
                    || !record.topic().equals(firstValid.topic()))) {
                singlePartition = false;
            }
        }

        if (firstValid == null) {
            return;
        }

        if (singlePartition) {
            String markerId = firstValid.topic() + ":" + firstValid.partition();
            if (allValid) {
                applyService.applyPartitionBatch(records, markerId);
                return;
            }
            List<ConsumerRecord<String, CompanyRewardDelta>> filtered = new ArrayList<>(records.size());
            for (ConsumerRecord<String, CompanyRewardDelta> record : records) {
                if (valid(record.value())) {
                    filtered.add(record);
                }
            }
            applyService.applyPartitionBatch(filtered, markerId);
            return;
        }

        // Preserve strict single-partition ordering and marker isolation when a poll spans multiple partitions.
        Map<TopicPartition, List<ConsumerRecord<String, CompanyRewardDelta>>> byPartition = new LinkedHashMap<>();
        for (ConsumerRecord<String, CompanyRewardDelta> record : records) {
            if (!valid(record.value())) {
                continue;
            }
            byPartition.computeIfAbsent(
                    new TopicPartition(record.topic(), record.partition()),
                    ignored -> new ArrayList<>()
            ).add(record);
        }
        for (Map.Entry<TopicPartition, List<ConsumerRecord<String, CompanyRewardDelta>>> entry : byPartition.entrySet()) {
            TopicPartition tp = entry.getKey();
            applyService.applyPartitionBatch(entry.getValue(), tp.topic() + ":" + tp.partition());
        }
    }

    private boolean valid(CompanyRewardDelta delta) {
        return delta != null
                && delta.schemaVersion() == CompanyRewardDelta.SCHEMA_VERSION
                && delta.deltaId() != null
                && !delta.deltaId().isBlank()
                && delta.companyId() != null
                && !delta.companyId().isBlank()
                && delta.deltaPoints() > 0;
    }
}
