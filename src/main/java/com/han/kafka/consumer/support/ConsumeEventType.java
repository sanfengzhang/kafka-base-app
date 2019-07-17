package com.han.kafka.consumer.support;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
public enum ConsumeEventType {

    /**消费组全部停掉*/
    DESTORY,

    /**消费组暂停*/
    PAUSE,

    /**唤醒暂停*/

    RESUME,

    /**增加新的消费者*/
    ADD_CONSUMER,

    /**创建新的的消费组*/
    CREATE_NEW_CONSUME_GROUP,

    /**topic级别的信息改变*/
    TOPIC_CONFIG_CHANG;

}
