package com.han.kafka.consumer.Utils;

import com.han.kafka.consumer.api.ConsumerOffsetInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupListing;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author: Hanl
 * @date :2019/5/30
 * @desc:
 */
@Slf4j
public class KafkaOptionUtil {

    public static List<ConsumerOffsetInfo> getAllTopicConsumerOffsetNew() {
        List<ConsumerOffsetInfo> result = new LinkedList<ConsumerOffsetInfo>();
        Properties clientProps = new Properties();
        clientProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "");
        clientProps.put("client.id", "expandKafkaPartitionClient");
        AdminClient client = AdminClient.create(clientProps);
        try {
            client.listConsumerGroups().all().get(30, TimeUnit.SECONDS).forEach(new Consumer<ConsumerGroupListing>() {
                @Override
                public void accept(ConsumerGroupListing consumerGroupListing) {
                    try {
                        String groupId = consumerGroupListing.groupId();
                        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = client.listConsumerGroupOffsets(groupId).
                                partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
                        topicPartitionOffsetAndMetadataMap.forEach((k, v) -> {
                            ConsumerOffsetInfo consumerOffsetInfo = new ConsumerOffsetInfo(groupId, k.topic(), k.partition(), v.offset(), 0L);
                            result.add(consumerOffsetInfo);
                        });
                    } catch (Exception e) {
                        log.error("Get topic partition OffsetAndMetadata failed", e);
                    }
                }
            });
        } catch (Exception e) {
            log.error("Get topic partition OffsetAndMetadata failed", e);
        } finally {
            if (null != client) {
                client.close();
            }
        }
        return result;
    }
}
