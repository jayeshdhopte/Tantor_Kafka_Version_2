package io.translab.tantor.server.dto;

public class PartitionLagDto {
    private String topic;
    private int partition;
    private long currentOffset;
    private long logEndOffset;
    private long lag;

    public PartitionLagDto() {}

    public PartitionLagDto(String topic, int partition, long currentOffset, long logEndOffset, long lag) {
        this.topic = topic;
        this.partition = partition;
        this.currentOffset = currentOffset;
        this.logEndOffset = logEndOffset;
        this.lag = lag;
    }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public int getPartition() { return partition; }
    public void setPartition(int partition) { this.partition = partition; }

    public long getCurrentOffset() { return currentOffset; }
    public void setCurrentOffset(long currentOffset) { this.currentOffset = currentOffset; }

    public long getLogEndOffset() { return logEndOffset; }
    public void setLogEndOffset(long logEndOffset) { this.logEndOffset = logEndOffset; }

    public long getLag() { return lag; }
    public void setLag(long lag) { this.lag = lag; }
}
