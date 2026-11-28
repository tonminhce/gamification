package com.vnpt.einvoice.gamification.snapshot;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Administrative endpoints for Stage 3 disaster recovery: barrier-consistent snapshots and
 * Kafka suffix replay rebuilds.
 */
@RestController
@RequestMapping("/api/admin/snapshots")
public class SnapshotController {

    private final LeaderboardSnapshotService snapshotService;

    public SnapshotController(LeaderboardSnapshotService snapshotService) {
        this.snapshotService = snapshotService;
    }

    @PostMapping
    ResponseEntity<LeaderboardSnapshot.SnapshotMetadata> capture() {
        return ResponseEntity.ok(snapshotService.captureSnapshot());
    }

    @GetMapping("/latest")
    ResponseEntity<LeaderboardSnapshot.SnapshotMetadata> latest() {
        LeaderboardSnapshot.SnapshotMetadata latest = snapshotService.latestSnapshotMetadata();
        return latest != null ? ResponseEntity.ok(latest) : ResponseEntity.notFound().build();
    }

    @PostMapping("/rebuild")
    ResponseEntity<LeaderboardSnapshot.RebuildReport> rebuild(
            @RequestParam(required = false) String snapshotId,
            @RequestParam(defaultValue = "false") boolean cold
    ) {
        return ResponseEntity.ok(snapshotService.rebuild(snapshotId, cold));
    }
}
