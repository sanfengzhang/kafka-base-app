package com.han.kafka.consumer.v2;

import com.han.kafka.consumer.support.ConsumeEventType;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author: Hanl
 * @date :2019/7/16
 * @desc:
 */
public class ConsumerEventDispatcher {

    /**
     * 接受消费者管理事件
     */
    private BlockingQueue<ConsumerEventV2> eventQueue = new LinkedBlockingQueue<>(200);

    private JobConsumerManager jobConusmerManager;

    public void handleEvent() {
        try {
            while (true) {
                ConsumerEventV2 event = eventQueue.poll(10, TimeUnit.SECONDS);
                if (null != event) {
                    ConsumeEventType type = event.getConsumeEventType();
                    if (type == ConsumeEventType.ADD_CONSUMER) {

                    } else if (type == ConsumeEventType.TOPIC_CONFIG_CHANG) {


                    } else if (type == ConsumeEventType.CREATE_NEW_CONSUME_GROUP) {


                    } else if (type == ConsumeEventType.PAUSE) {

                        jobConusmerManager.getJobConsumer(event.getJobName()).pause(event.getIds());

                    }else if (type == ConsumeEventType.RESUME){

                        jobConusmerManager.getJobConsumer(event.getJobName()).resume(event.getIds());
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
