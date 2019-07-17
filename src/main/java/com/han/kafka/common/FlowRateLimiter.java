package com.han.kafka.common;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
public interface FlowRateLimiter {

    public void setRate(long rate);

    public void getRate();

    public void acquire(int permits);


    /**
     * kafka按取出按数据字节大小限流
     */
    public int getRecordBatchSize(ConsumerRecords<String, String> records );

    public int getRecordCount(ConsumerRecords<String, String> records);

}
