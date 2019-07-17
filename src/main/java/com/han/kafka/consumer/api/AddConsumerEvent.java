package com.han.kafka.consumer.api;

import com.han.kafka.consumer.support.ConsumeEventType;
import lombok.Data;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
@Data
public class AddConsumerEvent extends ConsumerEvent {

    private String consumerGroupId;

    private int addNum;

    public AddConsumerEvent() {
        consumeEventType = ConsumeEventType.ADD_CONSUMER;
    }

    public AddConsumerEvent(String consumerGroupId, int addNum) {
        super();
        this.consumerGroupId = consumerGroupId;
        this.addNum = addNum;
    }
}
