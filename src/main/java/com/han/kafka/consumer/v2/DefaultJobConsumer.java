package com.han.kafka.consumer.v2;

import com.han.kafka.consumer.support.PartitionOffset;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author: Hanl
 * @date :2019/7/16
 * @desc:
 */
public class DefaultJobConsumer extends JobConsumer {

    private String jobName;

    private Class<RecordFunction> useFunction;

    private volatile int numberOfConsumer;

    private Set<String> topics;

    private List<Map<String, List<PartitionOffset>>> topicPartitionOffsets;

    private Properties kafkaProperties;

    public DefaultJobConsumer(String jobName, Properties kafkaProperties, Class<RecordFunction> useFunction, Set<String> topics) {

        this(jobName, kafkaProperties, useFunction, topics, 1, null);
    }

    public DefaultJobConsumer(String jobName, Properties kafkaProperties, Class<RecordFunction> useFunction, List<Map<String, List<PartitionOffset>>> topicPartitionOffsets) {

        this(jobName, kafkaProperties, useFunction, null, 1, topicPartitionOffsets);
    }

    public DefaultJobConsumer(String jobName, Properties kafkaProperties, Class<RecordFunction> useFunction, Set<String> topics, int numberOfConsumer, List<Map<String, List<PartitionOffset>>> topicPartitionOffsets) {
        this.jobName = jobName;
        if (0 == numberOfConsumer) {
            this.numberOfConsumer = getNumberOfConsumer();
        } else {
            this.numberOfConsumer = numberOfConsumer;
        }
        this.kafkaProperties = kafkaProperties;
        this.useFunction = useFunction;
        this.topics = topics;
        this.topicPartitionOffsets = topicPartitionOffsets;

        initConsumer();
    }


    private int getNumberOfConsumer() {

        return 0;
    }

    @Override
    public void initConsumer() {
        for (int i = 0; i < numberOfConsumer; i++) {
            createTask(i);
        }
    }

    private synchronized void createTask(int i) {
        ConsumerTask consumerTask = null;
        String taskId = jobName + "-#-" + i;
        if (topicPartitionOffsets != null && topicPartitionOffsets.size() == numberOfConsumer) {

            consumerTask = new ConsumerTask(taskId, kafkaProperties, topics, 1000L, useFunction, topicPartitionOffsets.get(i));
        } else if (topicPartitionOffsets == null) {

            consumerTask = new ConsumerTask(taskId, kafkaProperties, topics, 1000L, useFunction, null);
        } else {
            throw new IllegalArgumentException("the size of topicPartitionOffsets must be equal numberOfConsumer");
        }
        if (null != consumerTask) {
            this.getConsumers().put(taskId, consumerTask);
        }
    }

    @Override
    public void addNewConsumer() {

        if (topics.size() == 1) {
            numberOfConsumer += 1;
            createTask(numberOfConsumer);
            return;
        }

        throw new UnsupportedOperationException("The number of consumers who do not support multiple topic has increased");
    }

    @Override
    public void stop() {
        getConsumers().forEach((id, consumer) -> {

        });
    }
}
