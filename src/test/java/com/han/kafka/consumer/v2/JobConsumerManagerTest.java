package com.han.kafka.consumer.v2;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @author: Hanl
 * @date :2019/7/17
 * @desc:
 */
public class JobConsumerManagerTest {

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
    }


    @Test
    public void creatConsumers() throws Exception {

        JobConsumerManager jobConsumerManager = new JobConsumerManager();
        Set<String> set = new HashSet<>();
        set.add("test-leader");
        jobConsumerManager.creatJobConsumer("my-test", kafkaProps, RecordFunctionCase.class, set);
        jobConsumerManager.startJobConsumer("my-test");
        System.in.read();
    }
}
