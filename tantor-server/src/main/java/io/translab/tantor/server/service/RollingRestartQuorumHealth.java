package io.translab.tantor.server.service;

import org.apache.kafka.clients.admin.QuorumInfo;

/** Quorum timestamps share the leader's clock, not the management server's clock. */
final class RollingRestartQuorumHealth {
    private RollingRestartQuorumHealth() {}

    static boolean healthy(QuorumInfo quorum, boolean singleOverride) {
        if (quorum.leaderId() < 0 || (!singleOverride && quorum.voters().size() < 3)
                || quorum.voters().stream().noneMatch(v -> v.replicaId() == quorum.leaderId())) return false;
        long latestFetch = quorum.voters().stream().mapToLong(v -> v.lastFetchTimestamp().orElse(-1L)).max().orElse(-1L);
        return quorum.voters().stream().allMatch(v -> v.logEndOffset() >= quorum.highWatermark()
                && (v.replicaId() == quorum.leaderId() || (latestFetch >= 0
                && v.lastFetchTimestamp().orElse(-1L) >= latestFetch - 30000
                && v.lastCaughtUpTimestamp().orElse(-1L) >= 0
                && v.lastCaughtUpTimestamp().orElse(-1L) >= latestFetch - 30000)));
    }
}
