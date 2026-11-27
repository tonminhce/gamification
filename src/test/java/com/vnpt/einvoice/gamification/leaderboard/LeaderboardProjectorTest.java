package com.vnpt.einvoice.gamification.leaderboard;

import com.vnpt.einvoice.gamification.event.CompanyRewardDelta;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class LeaderboardProjectorTest {

    @Mock
    LeaderboardApplyService applyService;

    @Test
    void appliesDeltasForThePartition() {
        CompanyRewardDelta first = delta("D-1", "COMPANY-A", 10);
        CompanyRewardDelta second = delta("D-2", "COMPANY-B", 20);
        LeaderboardProjector projector = new LeaderboardProjector(applyService);

        projector.consume(List.of(record(first, 10), record(second, 11)));

        verify(applyService).applyDeltas(List.of(first, second), "company-reward-deltas:0");
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

    private ConsumerRecord<String, CompanyRewardDelta> record(CompanyRewardDelta delta, long offset) {
        return new ConsumerRecord<>("company-reward-deltas", 0, offset, delta.companyId(), delta);
    }
}