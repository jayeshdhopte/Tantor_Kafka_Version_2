package io.translab.tantor.server.service;

import org.apache.kafka.clients.admin.QuorumInfo;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.OptionalLong;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class RollingRestartQuorumHealthTest {
    private QuorumInfo.ReplicaState voter(int id, long offset, long fetched, long caught) {
        var voter = mock(QuorumInfo.ReplicaState.class);
        when(voter.replicaId()).thenReturn(id); when(voter.logEndOffset()).thenReturn(offset);
        when(voter.lastFetchTimestamp()).thenReturn(fetched < 0 ? OptionalLong.empty() : OptionalLong.of(fetched));
        when(voter.lastCaughtUpTimestamp()).thenReturn(caught < 0 ? OptionalLong.empty() : OptionalLong.of(caught));
        return voter;
    }
    private QuorumInfo quorum(long clock, long followerOffset, long followerCaught) {
        var quorum = mock(QuorumInfo.class);
        when(quorum.leaderId()).thenReturn(102); when(quorum.highWatermark()).thenReturn(100L);
        var voters = List.of(voter(102, 101, clock, clock), voter(101, followerOffset, clock - 100, followerCaught), voter(103, 100, clock - 200, clock - 200));
        when(quorum.voters()).thenReturn(voters);
        return quorum;
    }
    @Test void clockSkewDoesNotFailCaughtUpVoters() {
        for (long clock : List.of(System.currentTimeMillis() - 93000, System.currentTimeMillis() + 93000))
            assertThat(RollingRestartQuorumHealth.healthy(quorum(clock, 100, clock - 200), false)).isTrue();
    }
    @Test void laggingStaleAndMissingVotersAreRejected() {
        long clock = System.currentTimeMillis();
        assertThat(RollingRestartQuorumHealth.healthy(quorum(clock, 99, clock), false)).isFalse();
        assertThat(RollingRestartQuorumHealth.healthy(quorum(clock, 100, clock - 31000), false)).isFalse();
        assertThat(RollingRestartQuorumHealth.healthy(quorum(clock, 100, -1), false)).isFalse();
        var quorum = quorum(clock, 100, clock); when(quorum.leaderId()).thenReturn(999);
        assertThat(RollingRestartQuorumHealth.healthy(quorum, false)).isFalse();
        when(quorum.leaderId()).thenReturn(102); var leaderOnly = List.of(voter(102, 100, clock, clock)); when(quorum.voters()).thenReturn(leaderOnly);
        assertThat(RollingRestartQuorumHealth.healthy(quorum, false)).isFalse();
        assertThat(RollingRestartQuorumHealth.healthy(quorum, true)).isTrue();
    }
}
