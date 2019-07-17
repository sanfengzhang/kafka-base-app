package com.han.kafka.common;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author: Hanl
 * @date :2019/5/30
 * @desc:
 */
public class DefaultFlowRateLimiter implements FlowRateLimiter {

    private RateLimiter rateLimiter;

    public DefaultFlowRateLimiter(double localCountPerSecond) {
        rateLimiter = RateLimiter.create(localCountPerSecond);
    }

    @Override
    public void setRate(long rate) {

    }

    @Override
    public void getRate() {

    }

    @Override
    public void acquire(int permits) {
        rateLimiter.acquire(permits);
    }

    @Override
    public int getRecordBatchSize(ConsumerRecords<String, String> records) {
        int recordBatchSizeBytes = 0;
        for (ConsumerRecord<String, String> record : records) {
            // Null is an allowed value for the key
            if (record.key() != null) {
                recordBatchSizeBytes += record.key().getBytes().length;
            }
            recordBatchSizeBytes += record.value().getBytes().length;

        }
        return recordBatchSizeBytes;
    }

    @Override
    public int getRecordCount(ConsumerRecords<String, String> records) {

        return records.count();
    }
}
