package com.han.kafka.consumer.api;

import lombok.Data;

/**
 * @author: Hanl
 * @date :2019/5/30
 * @desc:
 */
@Data
public class PauseEvent extends ConsumerEvent {

    private String groupId;
}
