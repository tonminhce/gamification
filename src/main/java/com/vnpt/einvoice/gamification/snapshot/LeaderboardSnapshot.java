package com.vnpt.einvoice.gamification.snapshot;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public record LeaderboardSnapshot(
        String snapshotId,
        Instant capturedAt,
        String leaderboardKey,
        String deltaTopic,
        int partitionCount,
        Map<Integer, Long> partitionOffsets,
        List<SnapshotEntry> entries,
        long totalPoints,
        Map<Long, List<String>> processedDeltaIdsByDay
) {

    public SnapshotMetadata metadata() {
        return new SnapshotMetadata(
                snapshotId,
                capturedAt,
                deltaTopic,
                partitionCount,
                partitionOffsets,
                entries.size(),
                totalPoints
        );
    }

    public record SnapshotEntry(String companyId, long points) {
    }

    public record SnapshotMetadata(
            String snapshotId,
            Instant capturedAt,
            String deltaTopic,
            int partitionCount,
            Map<Integer, Long> partitionOffsets,
            int companyCount,
            long totalPoints
    ) {
    }

    public record RebuildReport(
            String snapshotId,
            boolean coldReplayFromKafka,
            int restoredCompanies,
            int restoredDeltaMarkers,
            long replayedKafkaRecords,
            int appliedKafkaRecords,
            long totalPointsAfterRebuild,
            long durationMs
    ) {
    }
}
