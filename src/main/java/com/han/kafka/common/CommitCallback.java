package com.han.kafka.common;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
public interface CommitCallback {

    /**
     * 异步接口
     * @param consumer
     * @param offsets
     */
    default void onSuccess(Consumer consumer, Map<TopicPartition, OffsetAndMetadata> offsets){

    }

    /**
     * 同步接口
     * @param consumer
     */
   default public void onSuccess(Consumer consumer){

   }


    default  public void onException(Throwable cause){

    }

    /**
     * 回调函数,来处理记录血缘关系,消息审计
     * 这里需要对id处理持久化，异步批量去处理
     * 对于处理时间不需要根据单条记录去做处理，依照kafka批次数据处理就好了
     */
    default void onLineage(ConsumerRecords<String, String> consumerRecord) {


    }
}
