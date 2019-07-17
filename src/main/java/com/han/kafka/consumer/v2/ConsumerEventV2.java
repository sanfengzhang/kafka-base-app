package com.han.kafka.consumer.v2;

import com.han.kafka.consumer.support.ConsumeEventType;
import lombok.Data;

/**
 * @author: Hanl
 * @date :2019/7/16
 * @desc:
 */
@Data
public class ConsumerEventV2 {

    private String jobName;

    private String[] ids;

    protected ConsumeEventType consumeEventType;
}
