package io.translab.tantor.server.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.translab.tantor.server.domain.Cluster;
import io.translab.tantor.server.domain.ClusterServiceAssignment;
import io.translab.tantor.server.repository.ClusterRepository;
import io.translab.tantor.server.repository.ClusterServiceAssignmentRepository;
import io.translab.tantor.server.repository.HostRepository;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class ClusterLiveLogsControllerTest {
    @Test
    void forwardsOnlyTheInventoryUnitAndAcceptsItsAuthenticatedAgentResult() throws Exception {
        var clusters = mock(ClusterRepository.class);
        var assignments = mock(ClusterServiceAssignmentRepository.class);
        var controller = new ClusterLiveLogsController(clusters, assignments, new ObjectMapper(), mock(HostRepository.class));
        String host = "agent-one";
        String token = "a".repeat(64);
        ReflectionTestUtils.setField(controller, "tokens", new ObjectMapper().writeValueAsString(Map.of(host, token)));
        var cluster = new Cluster();
        cluster.setId(UUID.randomUUID());
        cluster.setMode("KRAFT");
        when(clusters.findById(cluster.getId())).thenReturn(Optional.of(cluster));
        var assignment = new ClusterServiceAssignment();
        assignment.setId(UUID.randomUUID());
        assignment.setCluster(cluster);
        assignment.setRole("broker");
        assignment.setHostId(host);
        assignment.setConfigJson("{\"systemd_service\":\"tantor-kafka-broker-7\"}");
        when(assignments.findById(assignment.getId())).thenReturn(Optional.of(assignment));

        var services = new ArrayList<>(List.of(assignment));
        for (String role : List.of("schema_registry", "kafka_connect")) {
            var excluded = new ClusterServiceAssignment();
            excluded.setId(UUID.randomUUID());
            excluded.setCluster(cluster);
            excluded.setHostId(host);
            excluded.setRole(role);
            services.add(excluded);
            when(assignments.findById(excluded.getId())).thenReturn(Optional.of(excluded));
        }
        when(assignments.findByClusterId(cluster.getId())).thenReturn(services);
        var mvc = MockMvcBuilders.standaloneSetup(controller).build();
        String ui = "/api/v1/ui/clusters/" + cluster.getId() + "/live-logs";
        String agent = "/api/v1/agents/" + host + "/live-logs";
        Instant from = Instant.parse("2026-11-16T07:30:00Z");
        Instant to = from.plusSeconds(1800);
        mvc.perform(get(ui + "/sources")).andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1)).andExpect(jsonPath("$[0].role").value("broker"));
        for (var excluded : services.subList(1, services.size())) {
            var rejected = mvc.perform(get(ui).param("source", excluded.getId().toString())
                            .param("from", from.toString()).param("to", to.toString()))
                    .andExpect(request().asyncStarted()).andReturn();
            mvc.perform(asyncDispatch(rejected)).andExpect(status().isNotFound());
        }
        var result = mvc.perform(get(ui).param("source", assignment.getId().toString())
                        .param("from", from.toString()).param("to", to.toString()))
                .andExpect(request().asyncStarted()).andReturn();
        mvc.perform(get(agent)).andExpect(status().isUnauthorized());
        var job = mvc.perform(get(agent).header("X-Tantor-Log-Token", token))
                .andExpect(status().isOk()).andExpect(jsonPath("$.unit").value("tantor-kafka-broker-7.service"))
                .andReturn();
        var jobId = new ObjectMapper().readTree(job.getResponse().getContentAsString()).path("id").asText();
        mvc.perform(post(agent + "/" + UUID.randomUUID()).header("X-Tantor-Log-Token", token))
                .andExpect(status().isNotFound());
        mvc.perform(post(agent + "/" + jobId).header("X-Tantor-Log-Token", token).content("journal line"))
                .andExpect(status().isNoContent());
        mvc.perform(asyncDispatch(result)).andExpect(status().isOk())
                .andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(jsonPath("$.journal").value("journal line"));
    }
}
