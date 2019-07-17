package com.han.kafka.consumer.monitor;

/**
 * @author: Hanl
 * @date :2019/5/31
 * @desc: *
 * 1.消费状态、生产状态分析,判断当前任务消费、生产是否正常
 * 2.根据上述的一些指标参数、判断是否去要扩容、报警
 * 3.对一些thread是否可以重启
 */
public class KafkaMonitorTask implements Runnable {

    private ConsumerStatusCheckService consumerStatusCheckService;

    @Override
    public void run() {


    }
}
