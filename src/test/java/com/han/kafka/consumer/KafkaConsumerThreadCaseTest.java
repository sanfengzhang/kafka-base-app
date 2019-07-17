package com.han.kafka.consumer;

import com.google.common.util.concurrent.RateLimiter;
import com.han.kafka.consumer.api.PauseEvent;
import com.han.kafka.consumer.task.DefaultConsumerManager;
import com.han.kafka.consumer.api.CreateConsumerGroupEvent;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @author: Hanl
 * @date :2019/5/30
 * @desc:
 */
public class KafkaConsumerThreadCaseTest {

    Properties kafkaProps = new Properties();

    @Before
    public void setup() {

        PropertyConfigurator.configureAndWatch("src/main/resources/log4j.properties");
        kafkaProps.put("bootstrap.servers", "192.168.12.100:9092,192.168.12.101:9092,192.168.12.102:9092");
        kafkaProps.put("value.deserializer", StringDeserializer.class);
        kafkaProps.put("key.deserializer", StringDeserializer.class);
        kafkaProps.put("group.id", "test-flow-0");
        kafkaProps.put("enable.auto.commit", "true");
        kafkaProps.put("auto.commit.interval.ms", "3000");
        kafkaProps.put("session.timeout.ms", "10000");
        kafkaProps.put("auto.offset.reset", "earliest");
        kafkaProps.put("max.poll.records", "5");

        Set<String> topic = new HashSet<String>();
        topic.add("test-leader");

        kafkaProps.put("consume.tpics", topic);
    }

    @Test
    public void testCreatConsumer() throws Exception {
        DefaultConsumerManager manager = new DefaultConsumerManager();
        CreateConsumerGroupEvent event = new CreateConsumerGroupEvent();
        event.setConsumerClazzName("com.qianxin.msgflow.kafka.consumer.KafkaConsumerThreadCase");
        event.setGroupId("test-flow-0");
        event.setConsumerNum(2);
        event.setKafkaProperties(kafkaProps);
        manager.startConsumer(event);

        Thread.sleep(5000L);
        PauseEvent pauseEvent = new PauseEvent();
        pauseEvent.setGroupId("test-flow-0");

        manager.pause(pauseEvent);
        Thread.sleep(30000L);

        manager.resumeEvent(pauseEvent);
        System.in.read();
    }

    @Test
    public void testSeesiontimeOut()throws Exception {
        DefaultConsumerManager manager = new DefaultConsumerManager();
        CreateConsumerGroupEvent event = new CreateConsumerGroupEvent();
        event.setConsumerClazzName("com.qianxin.msgflow.kafka.consumer.KafkaConsumerThreadCase");
        event.setGroupId("test-flow-0");
        event.setConsumerNum(1);
        event.setKafkaProperties(kafkaProps);
        manager.startConsumer(event);


        System.in.read();

    }

    @Test
    public void testCreatClazz() throws Exception {
        Class clazz = Class.forName("com.han.kafka.consumer.KafkaConsumerThreadCase");
        Constructor[] constructors = clazz.getConstructors();
        Arrays.asList(constructors).forEach(constructor -> {
            System.out.println(constructor.toString());
        });
        Constructor constructor = clazz.getConstructor(new Class[]{String.class, Properties.class, Long.class});
        System.out.println(constructor.toString());
    }

    @Test
    public void testRateLimiter() {
        RateLimiter rateLimiter = RateLimiter.create(0.5);
        for (int i = 0; i < 10; i++) {
            rateLimiter.acquire(2);//每次循环需要等到令牌桶中有2个令牌,程序才能向下执行
            System.out.println(System.currentTimeMillis() + "  " + i);
        }

    }

}
