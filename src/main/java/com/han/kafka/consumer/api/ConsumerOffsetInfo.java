package com.han.kafka.consumer.api;

/**
 * 分区消费数据
 */
public class ConsumerOffsetInfo {

    // 消费者组名称
    private String groupName;
    // 消费Topic名称
    private String topic;
    // Partition编号
    private Integer partition;
    // 消费者Offset
    private Long partitionOffset;
    // 消费者Lag
    private Long consumerOffset;

    public ConsumerOffsetInfo() {
    }

    public ConsumerOffsetInfo(String groupName, String topic, Integer partition, Long partitionOffset, Long consumerOffset) {
        this.groupName = groupName;
        this.topic = topic;
        this.partition = partition;
        this.partitionOffset = partitionOffset;
        this.consumerOffset = consumerOffset;
    }

    @Override
    public String toString() {
        return "ConsumerOffsetInfo{" +
                "groupName='" + groupName + '\'' +
                ", topic='" + topic + '\'' +
                ", partition=" + partition +
                ", partitionOffset=" + partitionOffset +
                ", consumerOffset=" + consumerOffset +
                '}';
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getPartitionOffset() {
        return partitionOffset;
    }

    public void setPartitionOffset(Long partitionOffset) {
        this.partitionOffset = partitionOffset;
    }

    public Long getConsumerOffset() {
        return consumerOffset;
    }

    public void setConsumerOffset(Long consumerOffset) {
        this.consumerOffset = consumerOffset;
    }
}
