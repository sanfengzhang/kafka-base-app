package com.han.kafka.consumer.support;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
public enum StartupMode {
    /** Start from committed offsets in ZK / Kafka brokers of a specific consumer group (default). */
    GROUP_OFFSETS(KafkaTopicPartitionStateSentinel.GROUP_OFFSET),

    /** Start from the earliest offset possible. */
    EARLIEST(KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET),

    /** Start from the latest offset. */
    LATEST(KafkaTopicPartitionStateSentinel.LATEST_OFFSET),

    /**
     * Start from user-supplied timestamp for each partition.
     * Since this mode will have specific offsets to start with, we do not need a sentinel value;
     * using Long.MIN_VALUE as a placeholder.
     */
    TIMESTAMP(Long.MIN_VALUE),

    /**
     * Start from user-supplied specific offsets for each partition.
     * Since this mode will have specific offsets to start with, we do not need a sentinel value;
     * using Long.MIN_VALUE as a placeholder.
     */
    SPECIFIC_OFFSETS(Long.MIN_VALUE);

    /** The sentinel offset value corresponding to this startup mode. */
    private long stateSentinel;

    StartupMode(long stateSentinel) {
        this.stateSentinel = stateSentinel;
    }

    public long getStateSentinel() {
        return stateSentinel;
    }
}
