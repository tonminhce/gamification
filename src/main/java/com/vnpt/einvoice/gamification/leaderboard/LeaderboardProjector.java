package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class LeaderboardProjector {

    private static final Logger log = LoggerFactory.getLogger(LeaderboardProjector.class);

    private final LeaderboardApplyService applyService;

    public LeaderboardProjector(LeaderboardApplyService applyService) {
        this.applyService = applyService;
    }

    @KafkaListener(topics = "${app.kafka.delta-topic}", batch = "true")
    public void consume(List<ConsumerRecord<String, CompanyRewardDelta>> records) {
        if (records.isEmpty()) {
            return;
        }

        List<CompanyRewardDelta> validDeltas = new ArrayList<>(records.size());
        for (ConsumerRecord<String, CompanyRewardDelta> record : records) {
            CompanyRewardDelta delta = record.value();
            if (valid(delta)) {
                validDeltas.add(delta);
            } else {
                log.warn("Skipping invalid company reward delta at offset {}", record.offset());
            }
        }
        if (validDeltas.isEmpty()) {
            return;
        }

        ConsumerRecord<String, CompanyRewardDelta> first = records.getFirst();
        String markerId = first.topic() + ":" + first.partition();
        applyService.applyDeltas(validDeltas, markerId);
    }

    private boolean valid(CompanyRewardDelta delta) {
        return delta != null
                && delta.deltaId() != null
                && !delta.deltaId().isBlank()
                && delta.companyId() != null
                && !delta.companyId().isBlank()
                && delta.deltaPoints() > 0;
    }
}