package com.han.kafka.consumer.support;

import lombok.Data;

import java.util.Map;

/**
 * @author: Hanl
 * @date :2019/5/29
 * @desc:
 */
@Data
public class PartitionOffset {

    private int partitionId;

    private long offset;


}
