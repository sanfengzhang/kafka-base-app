package com.han.kafka.consumer.api;

import com.han.kafka.consumer.support.ConsumeEventType;
import lombok.Data;

import java.io.Serializable;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
@Data
public class ConsumerEvent implements Serializable {

    private int id;

    protected ConsumeEventType consumeEventType;
}
