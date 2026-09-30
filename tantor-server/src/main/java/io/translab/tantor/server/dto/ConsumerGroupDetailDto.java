package io.translab.tantor.server.dto;

import java.util.List;

public class ConsumerGroupDetailDto {
    private String groupId;
    private String state;
    private List<MemberAssignmentDto> members;

    public ConsumerGroupDetailDto() {}

    public ConsumerGroupDetailDto(String groupId, String state, List<MemberAssignmentDto> members) {
        this.groupId = groupId;
        this.state = state;
        this.members = members;
    }

    public String getGroupId() { return groupId; }
    public void setGroupId(String groupId) { this.groupId = groupId; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public List<MemberAssignmentDto> getMembers() { return members; }
    public void setMembers(List<MemberAssignmentDto> members) { this.members = members; }
}
