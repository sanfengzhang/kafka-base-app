package com.han.kafka.consumer.task;

import com.han.kafka.consumer.api.*;
import com.han.kafka.consumer.support.ConsumeEventType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
@Slf4j
public class DefaultConsumerManager implements ConsumerManager, Runnable {

    /**
     * 缓存本地的所有consumer对应的消费topic信息
     */
    private final Map<String, List<TopicPartition>> localConsumerTopicPartition = new ConcurrentHashMap<String, List<TopicPartition>>();

    /**
     * 消费组管理
     */
    private final Map<String, List<BaseKafkaConsumer>> localConsumers = new ConcurrentHashMap<String, List<BaseKafkaConsumer>>();

    /**
     * 接受消费者管理事件
     */
    private BlockingQueue<ConsumerEvent> eventQueue = new LinkedBlockingQueue<>(200);

    public DefaultConsumerManager() {


    }

    @Override
    public void run() {

    }

    @Override
    public void subscribeConsumeChange() {

    }

    @Override
    public void handleEvent() {
        try {
            while (true) {
                ConsumerEvent event = eventQueue.poll(10, TimeUnit.SECONDS);
                if (null != event) {
                    ConsumeEventType type = event.getConsumeEventType();
                    if (type == ConsumeEventType.ADD_CONSUMER) {

                    } else if (type == ConsumeEventType.TOPIC_CONFIG_CHANG) {
                        ConsumerConfigChangeEvent eventNew = (ConsumerConfigChangeEvent) event;
                        String groupId = eventNew.getGroupId();
                        localConsumers.get(groupId).forEach(cosumer -> {
                            cosumer.acceptEvent(eventNew);
                        });

                    } else if (type == ConsumeEventType.CREATE_NEW_CONSUME_GROUP) {
                        CreateConsumerGroupEvent eventNew = (CreateConsumerGroupEvent) event;
                        startConsumer(eventNew);
                    } else if (type == ConsumeEventType.PAUSE) {

                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建新的consumer
     */
    public void startConsumer(CreateConsumerGroupEvent event) {
        try {
            int num = event.getConsumerNum();
            List<BaseKafkaConsumer> list = new ArrayList<BaseKafkaConsumer>();
            String groupId = event.getGroupId();
            for (int i = 0; i < num; i++) {
                Class clazz = Class.forName(event.getConsumerClazzName());
                BaseKafkaConsumer baseKafkaConsumer = (BaseKafkaConsumer) clazz.getConstructor(new Class[]{String.class, Properties.class, Long.class, Double.class}).
                        newInstance(new Object[]{groupId + "--" + i, event.getKafkaProperties(), 100L, 10D});
                baseKafkaConsumer.start();
                list.add(baseKafkaConsumer);
            }

            localConsumers.put(groupId, list);

        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public void pause(PauseEvent event) {
        List<BaseKafkaConsumer> list = localConsumers.get(event.getGroupId());
        list.forEach(baseKafkaConsumer -> {
            String resp = baseKafkaConsumer.pauseConsumer();
            log.info("Get response from pause consumer,resp={}", resp);
        });

    }

    public void resumeEvent(PauseEvent event) {
        List<BaseKafkaConsumer> list = localConsumers.get(event.getGroupId());
        list.forEach(baseKafkaConsumer -> {
            String resp = baseKafkaConsumer.resumeConsumer();
            log.info("Get response from resume consumer,resp={}", resp);
        });
    }


}
