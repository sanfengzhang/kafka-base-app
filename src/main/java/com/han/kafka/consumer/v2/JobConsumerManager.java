package com.han.kafka.consumer.v2;

import com.han.kafka.consumer.support.PartitionOffset;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author: Hanl
 * @date :2019/7/16
 * @desc:
 */
public class JobConsumerManager<T> {

    private Map<String, JobConsumer> consumerMap = new ConcurrentHashMap<String, JobConsumer>();

    public  void  creatJobConsumer(String jobName, Properties kafkaProperties, Class<RecordFunction> useFunction, Set<String> topics) {
        JobConsumer jobConsumer = new DefaultJobConsumer(jobName, kafkaProperties, useFunction, topics);
        consumerMap.put(jobName, jobConsumer);
    }

    public  void  creatJobConsumer(String jobName, Properties kafkaProperties, Class<RecordFunction> useFunction, List<Map<String, List<PartitionOffset>>> topicPartitionOffsets) {
        JobConsumer jobConsumer = new DefaultJobConsumer(jobName, kafkaProperties, useFunction, topicPartitionOffsets);
        consumerMap.put(jobName, jobConsumer);
    }

    public void startJobConsumer(String jobName) {
        consumerMap.get(jobName).getConsumers().forEach((id, consumer) -> {
            consumer.start();
        });
    }

    public void startJobConsumerById(String jobName, String... ids) {
        consumerMap.get(jobName).getConsumers().forEach((id, consumer) -> {
            for (String startId : ids) {
                if (startId == id) {
                    consumer.start();
                }
            }
        });
    }


    public void stopJobConsumer(String jobName) {

        consumerMap.get(jobName).stop();
    }

    public JobConsumer getJobConsumer(String jobName) {

        return consumerMap.get(jobName);
    }

}
