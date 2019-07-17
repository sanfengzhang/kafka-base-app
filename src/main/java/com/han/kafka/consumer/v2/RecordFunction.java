package com.han.kafka.consumer.v2;

import com.han.kafka.consumer.Utils.AppException;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author: Hanl
 * @date :2019/7/17
 * @desc:
 */
public interface RecordFunction {

    public  void handleRecords(ConsumerRecords<String, String> records) throws AppException;
}
