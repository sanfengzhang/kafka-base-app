package com.han.kafka.consumer.api;

/**
 * @Desc 定义kafka扩容策略接口
 * @Author Hanl
 */
public interface KafkaPartitionExpandStrategy {

    public void expand();

    /**
     *
     * @param topic 分区名称
     * @param addNumPartitions 增加的数量
     */
    public void doExpand(String topic, int addNumPartitions);
}
