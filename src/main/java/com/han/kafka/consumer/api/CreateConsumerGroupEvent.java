package com.han.kafka.consumer.api;

import com.han.kafka.consumer.support.PartitionOffset;
import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @author: Hanl
 * @date :2019/5/30
 * @desc:
 */
@Data
public class CreateConsumerGroupEvent extends ConsumerEvent {

    //需要创建的消费者的实例类,kafka--es,hive
    private String consumerClazzName;

    //指定消费组ID
    private String groupId;

    
    private Set<String> topics;

    //指定在该应用实例中创建的消费者数量
    private int consumerNum;

    //消费哪些topic和指定的partition,offset
    private Map<String, List<PartitionOffset>> topicPartitionOffsets;

    //kafka的配置
    private Properties kafkaProperties;


}
