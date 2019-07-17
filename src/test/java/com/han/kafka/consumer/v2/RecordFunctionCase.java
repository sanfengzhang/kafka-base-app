package com.han.kafka.consumer.v2;

import com.han.kafka.consumer.Utils.AppException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author: Hanl
 * @date :2019/7/17
 * @desc:
 */
@Slf4j
public class RecordFunctionCase implements RecordFunction {

    @Override
    public void handleRecords(ConsumerRecords<String, String> records) throws AppException {
        records.forEach(consumerRecord -> {
            log.info("currentTime={},partition={},offset={}",
                    new Object[]{System.currentTimeMillis(), consumerRecord.partition(), consumerRecord.offset()});
        });
    }
}
