package io.translab.tantor.server.dto;

import java.util.List;

public class MemberAssignmentDto {
    private String memberId;
    private String clientId;
    private String host;
    private List<PartitionLagDto> partitions;

    public MemberAssignmentDto() {}

    public MemberAssignmentDto(String memberId, String clientId, String host, List<PartitionLagDto> partitions) {
        this.memberId = memberId;
        this.clientId = clientId;
        this.host = host;
        this.partitions = partitions;
    }

    public String getMemberId() { return memberId; }
    public void setMemberId(String memberId) { this.memberId = memberId; }

    public String getClientId() { return clientId; }
    public void setClientId(String clientId) { this.clientId = clientId; }

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public List<PartitionLagDto> getPartitions() { return partitions; }
    public void setPartitions(List<PartitionLagDto> partitions) { this.partitions = partitions; }
}
