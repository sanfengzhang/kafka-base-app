package com.han.kafka.consumer.support;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
public class KafkaTopicPartitionStateSentinel {
    /**
     * Magic number that defines an unset offset.
     */
    public static final long OFFSET_NOT_SET = -915623761776L;

    /**
     * Magic number that defines the partition should start from the earliest offset.
     *
     * <p>This is used as a placeholder so that the actual earliest offset can be evaluated lazily
     * when the partition will actually start to be read by the consumer.
     */
    public static final long EARLIEST_OFFSET = -915623761775L;

    /**
     * Magic number that defines the partition should start from the latest offset.
     *
     * <p>This is used as a placeholder so that the actual latest offset can be evaluated lazily
     * when the partition will actually start to be read by the consumer.
     */
    public static final long LATEST_OFFSET = -915623761774L;

    /**
     * Magic number that defines the partition should start from its committed group offset in Kafka.
     *
     * <p>This is used as a placeholder so that the actual committed group offset can be evaluated lazily
     * when the partition will actually start to be read by the consumer.
     */
    public static final long GROUP_OFFSET = -915623761773L;

    public static boolean isSentinel(long offset) {
        return offset < 0;
    }
}
