package com.han.kafka.consumer.api;

import com.han.kafka.consumer.support.ConsumeEventType;
import lombok.Data;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
@Data
public class DestoryConsumerEvent extends ConsumerEvent {

    private String groupId;

    public DestoryConsumerEvent() {

        this.consumeEventType = ConsumeEventType.DESTORY;
    }

    public DestoryConsumerEvent(String groupId) {
        super();
        this.groupId = groupId;
    }
}
