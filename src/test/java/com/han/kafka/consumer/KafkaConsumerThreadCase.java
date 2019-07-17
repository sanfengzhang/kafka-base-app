package com.han.kafka.consumer;

import com.han.kafka.consumer.Utils.AppException;
import com.han.kafka.consumer.task.BaseKafkaConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Properties;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
@Slf4j
public class KafkaConsumerThreadCase extends BaseKafkaConsumer {

    public KafkaConsumerThreadCase(String threadName, Properties kafkaProperties, Long pollTimeout, Double rateLimiter) {
        super(threadName, kafkaProperties, pollTimeout, rateLimiter);
    }

    @Override
    public void handleRecords(ConsumerRecords<String, String> records)throws AppException {
        records.forEach(consumerRecord -> {
            log.info("currentTime={},partition={},offset={}",
                    new Object[]{System.currentTimeMillis(), consumerRecord.partition(), consumerRecord.offset()});
        });
    }
}
