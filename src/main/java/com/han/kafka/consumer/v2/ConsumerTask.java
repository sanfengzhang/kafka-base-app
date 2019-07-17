package com.han.kafka.consumer.v2;

import com.han.kafka.consumer.support.PartitionOffset;
import com.han.kafka.common.CommitCallback;
import com.han.kafka.common.FlowRateLimiter;
import com.han.kafka.consumer.api.ConsumerConfigChangeEvent;
import com.han.kafka.consumer.api.ConsumerEvent;
import com.han.kafka.consumer.support.KafkaTopicPartitionStateSentinel;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc: 设计需求如下：
 * 1.动态创建, 销毁消费者任务,对消费任务进行自动化管理
 * 2.可以结合实际生产情况对任务进行管理,解决一些需要按offset消费的任务
 * 3.对任务限流按流量字节大小,数据量进行限流目的是为了任务持续稳定执行,
 * 在资源紧张的时候可以适当的进行限流降低程序压力,按byte大小更有意义
 * 4.对消费者任务的度量,比如根据当前任务消费的record计算任务线程的EPS
 * 实时监控消费者的压力
 * 5.在进行动态扩展分区的时候,能结合任务动态创建按资源进行适当增加消费者
 * 提高处理能力
 * <p>
 * 功能架构设计：
 * 1.将每一组的的线程执行任务抽象成一个Job,在Job下面有多个consumer的设计
 * 2.Job中可以对每个Consumer消费的Topic的分区的指定初始化
 * 3.通过Job定义Job去初始化consumer,JobConsumer是一组消费线程的管理实现
 * 4.通过事件分发机制去管理consumer的消费情况
 * 5.
 */
@Slf4j
public class ConsumerTask extends Thread {

    private volatile boolean running = true;

    protected KafkaConsumer<String, String> consumer;

    private final Properties kafkaProperties;

    private final long pollTimeout;

    private boolean autoCommit;

    private final Object consumerReassignmentLock;

    private FlowRateLimiter rateLimiter;

    protected CommitCallback commitCallback;

    private volatile boolean shouldPaus = false;

    private final Lock pauseLock = new ReentrantLock();

    private Condition pauseLockCondition = pauseLock.newCondition();

    private final BlockingQueue<ConsumerEvent> consumerEventQueue = new LinkedBlockingQueue<ConsumerEvent>(100);

    private Class<RecordFunction> functionClass;

    private RecordFunction recordFunction;

    private Map<String, List<PartitionOffset>> topicPartitionOffsets;

    private Set<String> topics;


    public ConsumerTask(String threadName, Properties kafkaProperties,Set<String> topics,  Long pollTimeout, Class<RecordFunction> functionClass,
                        Map<String, List<PartitionOffset>> topicPartitionOffsets) {
        super(threadName);
        this.kafkaProperties = kafkaProperties;
        this.topics=topics;
        this.pollTimeout = pollTimeout;
        this.consumerReassignmentLock = new Object();
        this.topicPartitionOffsets = topicPartitionOffsets;
        this.functionClass = functionClass;
        try {
            recordFunction = functionClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        if (!running) {
            return;
        }
        ConsumerRecords<String, String> records = null;

        try {
            this.consumer = getConsumer(kafkaProperties);
            if (topicPartitionOffsets != null && topicPartitionOffsets.size() > 0) {
                reassignPartition(topicPartitionOffsets);
            }
        } catch (Throwable t) {
            log.error("create consumer failed", t);
        }

        try {
            while (running) {
                if (shouldPaus) {
                    pauseLock.lockInterruptibly();
                    pauseLockCondition.await();
                    pauseLock.unlock();
                }
                //先获取是否有更改Kafka消费配置事件
                if (consumerEventQueue.size() > 0) {
                    try {
                        ConsumerConfigChangeEvent event = (ConsumerConfigChangeEvent) consumerEventQueue.poll(10, TimeUnit.NANOSECONDS);
                        reassignPartition(event.getTopicPartitionOffsets());
                    } catch (InterruptedException e) {
                        log.warn("reassignPartition failed:" + e);
                    }
                }

                //从kafka获取记录
                records = getRecordsFromKafka();

                //FIXME 这里可以将consume poll线程和业务逻辑线程进行分离?
                recordFunction.handleRecords(records);
                if (autoCommit) {//只支持同步提交、异步提交数据一致性难以保障

                    this.commitCallback.onSuccess(consumer);
                }
            }
        } catch (Throwable t) {

            log.error("failed", t);
        }
    }

    public synchronized String pauseConsumer() {
        if (shouldPaus) {
            return "kafka consumer thread had pause!";
        }
        shouldPaus = true;
        log.info("pause current consumer");
        return "success";
    }

    public String resumeConsumer() {
        try {
            pauseLock.lockInterruptibly();
            if (!shouldPaus) {
                return "kafka consumer thread had resume!";
            }
            shouldPaus = false;
            pauseLockCondition.signal();
            log.info("resume current consumer");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            pauseLock.unlock();
        }
        return "success";
    }


    KafkaConsumer<String, String> getConsumer(Properties kafkaProperties) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(topics);
        return consumer;
    }

    void reassignPartition(Map<String, List<PartitionOffset>> newPartitions) {
        final KafkaConsumer<String, String> consumerTmp;
        synchronized (consumerReassignmentLock) {
            consumerTmp = this.consumer;
            this.consumer = null;
        }

        final Map<TopicPartition, Long> oldPartitionAssignmentsToPosition = new HashMap<>();

        for (TopicPartition oldPartition : consumerTmp.assignment()) {
            oldPartitionAssignmentsToPosition.put(oldPartition, consumerTmp.position(oldPartition));
        }

        final List<TopicPartition> newPartitionAssignments =
                new ArrayList<>(newPartitions.size() + oldPartitionAssignmentsToPosition.size());
        newPartitionAssignments.addAll(oldPartitionAssignmentsToPosition.keySet());
        convertKafkaPartitions(newPartitions, newPartitionAssignments);

        consumerTmp.assign(newPartitionAssignments);

        // 获取老分区的消费位置
        for (Map.Entry<TopicPartition, Long> oldPartitionToPosition : oldPartitionAssignmentsToPosition.entrySet()) {
            consumerTmp.seek(oldPartitionToPosition.getKey(), oldPartitionToPosition.getValue());
        }

        newPartitions.forEach((topic, partitionOffsets) -> {
            for (PartitionOffset newPartitionState : partitionOffsets) {
                TopicPartition topicPartition = new TopicPartition(topic, newPartitionState.getPartitionId());
                Collection<TopicPartition> collection = new ArrayList<>();
                collection.add(topicPartition);
                if (newPartitionState.getOffset() == KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET) {
                    consumerTmp.seekToBeginning(collection);
                    newPartitionState.setOffset(consumerTmp.position(topicPartition) - 1);
                } else if (newPartitionState.getOffset() == KafkaTopicPartitionStateSentinel.LATEST_OFFSET) {
                    consumerTmp.seekToEnd(collection);
                    newPartitionState.setOffset(consumerTmp.position(topicPartition) - 1);
                } else if (newPartitionState.getOffset() == KafkaTopicPartitionStateSentinel.GROUP_OFFSET) {
                    // the KafkaConsumer by default will automatically seek the consumer position
                    // to the committed group offset, so we do not need to do it.

                    newPartitionState.setOffset(consumerTmp.position(topicPartition) - 1);
                } else {
                    consumerTmp.seek(topicPartition, newPartitionState.getOffset() + 1);
                }
            }
        });

        synchronized (consumerReassignmentLock) {
            this.consumer = consumerTmp;
        }
    }

    void convertKafkaPartitions(Map<String, List<PartitionOffset>> newPartitions, List<TopicPartition> newPartitionAssignments) {
        newPartitions.forEach((topic, partitionOffsets) -> {
            partitionOffsets.forEach(partitionOffset -> {
                TopicPartition topicPartition = new TopicPartition(topic, partitionOffset.getPartitionId());
                newPartitionAssignments.add(topicPartition);
            });
        });
    }

    void acceptEvent(ConsumerConfigChangeEvent event) {

        consumerEventQueue.offer(event);
    }

    public Set<TopicPartition> assignment() {

        return this.consumer.assignment();
    }

    protected ConsumerRecords<String, String> getRecordsFromKafka() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));
        //NOTE:1.因为kafka poll一次是批量获取record数量,这个数量也是根据下游的业务处理情况来定的,不能
        //在超过sessiontimeout的时间内还没处理完,假设下游2000EPS/S,那么批次获取的情况<2000比较合适
        //2.在使用计数限流的时候如何设置,rateLimiter生产的速度?假设一批取500个record,那么rateLimiter?
        //假设我期望速度是1000EPS/S
        //,其实在这里就直接将rateLimiter=1000就好了.
        //在没有rateLimiter的时候其实处理速度限于业务逻辑的处理,有rateLimiter的时候,在获取recorde交给业务处理的时候
        //会有一个限制,例如:保持在1秒内发送1000个record给业务逻辑线程处理,在这里是否将kafka消费线程和业务处理线程
        //进行分离?
        if (rateLimiter != null && !records.isEmpty()) {
            int bytesRead = rateLimiter.getRecordCount(records);
            rateLimiter.acquire(bytesRead);//每次需要消耗bytesRead个令牌,如果桶内少于bytesRead则线程需要等待到有bytesRead
        }
        return records;
    }
}
