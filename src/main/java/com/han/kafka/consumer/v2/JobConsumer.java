package com.han.kafka.consumer.v2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: Hanl
 * @date :2019/7/16
 * @desc: 主要是针对一组相同任务的的consumer集合、定义一个消费Job
 * 其实也可以考虑本地部署、或者分布是部署的消费任务、
 */
public abstract class JobConsumer {

    private Map<String, ConsumerTask> consumers=new ConcurrentHashMap<>();

    /**
     * 初始化Consumer的功能
     */
    public abstract void initConsumer();

    public abstract void addNewConsumer();

    public abstract void stop();

    public void pause(String... ids) {

        for (String id : ids) {
            getConsumer(id).pauseConsumer();
        }
    }


    public void pause() {

        consumers.forEach((id, consumer) -> {
            consumer.pauseConsumer();
        });
    }

    public void resume(String... ids) {

        for (String id : ids) {
            getConsumer(id).resumeConsumer();
        }

    }

    public void resume() {

        consumers.forEach((id, consumer) -> {
            consumer.resumeConsumer();
        });
    }


    public ConsumerTask getConsumer(String id) {

        return consumers.get(id);
    }

    public Map<String, ConsumerTask> getConsumers() {
        return consumers;
    }
}
