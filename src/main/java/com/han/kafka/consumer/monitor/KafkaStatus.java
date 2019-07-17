package com.han.kafka.consumer.monitor;

/**
 * @author: Hanl
 * @date :2019/5/31
 * @desc:
 */
public enum KafkaStatus {

    DATA_BACKLOG,//数据积压了,我们需要怎么去衡量分区中有多少数据未消费完成、并且当前的消费EPS计算需要多久

    DATA_DELAY,//数据延迟,和积压要区分，数据消费在分钟级别的延迟

    CONSUMER_DEAD,//消费线程dead状态了

    COSUMER_PROBLEM,//消费者存在问题

    CONSUMER_HEALTH;//消费正常，EPS稳定

}
