package com.han.kafka.consumer.monitor;


import com.han.kafka.consumer.api.ConsumerOffsetInfo;
import com.han.kafka.consumer.api.KafkaPartitionExpandStrategy;

import java.util.List;

/**
 * @Author: Hanl
 * @Date :2019/5/17
 * @Desc:获取kfaka分区扩容的一些参数指标信息
 */
public abstract class AbstractKafkaPartitionExpandStrategy implements KafkaPartitionExpandStrategy {


    public List<ConsumerOffsetInfo> fetchConsumerOffsetLag() {


        return null;
    }

    @Override
    public void doExpand(String topic, int addNumPartitions) {


    }


}
