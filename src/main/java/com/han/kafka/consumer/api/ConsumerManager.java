package com.han.kafka.consumer.api;

import com.han.kafka.consumer.task.BaseKafkaConsumer;

import java.util.List;
import java.util.Map;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:消费配置该表本地分发器
 */
public interface ConsumerManager {


    /**
     * 监听配置信息改变，外部重新分配消费配置信息*
     */
    public void subscribeConsumeChange();


    /**
     * 分发配置信息给对应的consumer
     */
    public void handleEvent();


    /**暂停线程*/
    public void pause(PauseEvent event);


    /**唤醒线程*/
    public void resumeEvent(PauseEvent event);


    default Map<String, List<BaseKafkaConsumer>> getConsumers(){return null;};


}
