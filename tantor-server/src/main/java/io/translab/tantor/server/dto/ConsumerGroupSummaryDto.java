package io.translab.tantor.server.dto;

public class ConsumerGroupSummaryDto {
    private String groupId;
    private String state;
    private int membersCount;
    private long totalLag;
    private String health;
    private long lastUpdated;

    public ConsumerGroupSummaryDto() {}

    public ConsumerGroupSummaryDto(String groupId, String state, int membersCount, long totalLag, String health, long lastUpdated) {
        this.groupId = groupId;
        this.state = state;
        this.membersCount = membersCount;
        this.totalLag = totalLag;
        this.health = health;
        this.lastUpdated = lastUpdated;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public int getMembersCount() {
        return membersCount;
    }

    public void setMembersCount(int membersCount) {
        this.membersCount = membersCount;
    }

    public long getTotalLag() {
        return totalLag;
    }

    public void setTotalLag(long totalLag) {
        this.totalLag = totalLag;
    }

    public String getHealth() {
        return health;
    }

    public void setHealth(String health) {
        this.health = health;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
}
