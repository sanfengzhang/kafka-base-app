package com.han.kafka.consumer.api;

import com.han.kafka.consumer.support.ConsumeEventType;
import com.han.kafka.consumer.support.PartitionOffset;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
@Data
public class ConsumerConfigChangeEvent extends ConsumerEvent {


    private String groupId;

    private Map<String, List<PartitionOffset>> topicPartitionOffsets;

    public ConsumerConfigChangeEvent() {

        this.consumeEventType = ConsumeEventType.TOPIC_CONFIG_CHANG;
    }

    public ConsumerConfigChangeEvent(String groupId, Map<String, List<PartitionOffset>> topicPartitionOffsets) {
        super();
        this.groupId = groupId;
        this.topicPartitionOffsets = topicPartitionOffsets;
    }
}
