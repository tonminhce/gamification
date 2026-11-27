package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class LeaderboardProjectorTest {

    @Mock
    LeaderboardApplyService applyService;

    @Test
    void appliesDeltasForSinglePartition() {
        ConsumerRecord<String, CompanyRewardDelta> first = record(delta("D-1", "COMPANY-A", 10), 0, 10);
        ConsumerRecord<String, CompanyRewardDelta> second = record(delta("D-2", "COMPANY-B", 20), 0, 11);
        LeaderboardProjector projector = new LeaderboardProjector(applyService);

        projector.consume(List.of(first, second));

        verify(applyService).applyPartitionBatch(List.of(first, second), "company-reward-deltas:0");
    }

    @Test
    void groupsMultiPartitionPollBatchByTopicPartition() {
        ConsumerRecord<String, CompanyRewardDelta> p0 = record(delta("D-1", "COMPANY-A", 10), 0, 10);
        ConsumerRecord<String, CompanyRewardDelta> p1 = record(delta("D-2", "COMPANY-B", 20), 1, 5);
        ConsumerRecord<String, CompanyRewardDelta> p0Next = record(delta("D-3", "COMPANY-C", 30), 0, 11);
        LeaderboardProjector projector = new LeaderboardProjector(applyService);

        projector.consume(List.of(p0, p1, p0Next));

        verify(applyService).applyPartitionBatch(List.of(p0, p0Next), "company-reward-deltas:0");
        verify(applyService).applyPartitionBatch(List.of(p1), "company-reward-deltas:1");
    }

    @Test
    void skipsInvalidDeltas() {
        CompanyRewardDelta invalidPoints = delta("D-BAD", "COMPANY-A", 0);
        CompanyRewardDelta wrongVersion = new CompanyRewardDelta("D-V", "COMPANY-A", 10, Instant.now(), Instant.now(), 99);
        LeaderboardProjector projector = new LeaderboardProjector(applyService);

        projector.consume(List.of(record(invalidPoints, 0, 1), record(wrongVersion, 0, 2)));

        verify(applyService, never()).applyPartitionBatch(anyList(), anyString());
    }

    private CompanyRewardDelta delta(String deltaId, String companyId, long points) {
        Instant now = Instant.now();
        return new CompanyRewardDelta(
                deltaId,
                companyId,
                points,
                now.minusMillis(500),
                now,
                CompanyRewardDelta.SCHEMA_VERSION
        );
    }

    private ConsumerRecord<String, CompanyRewardDelta> record(CompanyRewardDelta delta, int partition, long offset) {
        return new ConsumerRecord<>("company-reward-deltas", partition, offset, delta.companyId(), delta);
    }
}
