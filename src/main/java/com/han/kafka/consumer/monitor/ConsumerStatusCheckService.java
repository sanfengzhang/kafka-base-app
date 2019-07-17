package com.han.kafka.consumer.monitor;

import com.han.kafka.consumer.task.BaseKafkaConsumer;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: Hanl
 * @date :2019/5/30
 * @desc: 1.主要检查consumer是否消费正常包括:线程状态,offset处理速率
 * 2.针对这些问题consumer是否重启
 */
@Slf4j
public class ConsumerStatusCheckService {

    private int period;//单位秒

    private int count;//执行多少次

    private int kafkaEps;

    public boolean checkThreadStatus(BaseKafkaConsumer consumer) {
        if (consumer.isAlive()) {
            return true;
        }
        log.warn("check the thread is not alive!");
        return false;
    }

    /**
     * 1.计算生产者和消费者的offset变化
     * 2.然后进行求商计算，如果生产者Offset变化是>>0，而消费者offset<<0则存在问题
     * 3.数据积压：(生产者offset-消费者offfet)>EPS*3600，消费落后一个小时了
     * 4.消费者异常，消费者offset变化趋近于0，offset/EPS<<0,并且生产者offset有变化
     */
    public void checkOffsetChange(BaseKafkaConsumer consumer, List<Long> consumeOffsets, List<Long> lagOffsets) {
        int len = consumeOffsets.size();

        List<Long> cOffsets = new ArrayList<Long>();
        for (int i = 0; i < len - 1; i++) {

            long offsetChange = consumeOffsets.get(i + 1) - consumeOffsets.get(i);
            cOffsets.add(offsetChange);


        }

        List<Long> lOffsets = new ArrayList<Long>();
        int len1 = lagOffsets.size();
        for (int i = 0; i < len1 - 1; i++) {

            long offsetChange = lagOffsets.get(i + 1) - lagOffsets.get(i);
            lagOffsets.add(offsetChange);

        }

        long produceSum = 0;

        for (long l : lOffsets) {
            produceSum += produceSum;
        }

        long consumeSum = 0;

        for (long l : cOffsets) {
            consumeSum += consumeSum;
        }


    }

    /**
     * 10，40，42，45，55，129，200
     * 40，80，90，130，180，200，2000
     * 这个要取得更实时的效果应该在滑动时间窗口中计算，定时任务
     */
    private KafkaStatus checkBacklog(List<Long> consumeOffsets, List<Long> lagOffsets) {
        int len = consumeOffsets.size();
        long diffLast = lagOffsets.get(len - 1) - consumeOffsets.get(len - 1);

         //然后计算前六次的消费平均EPS，然后用diffLast/EPS6计算时长、标准EPS计算时长


        return KafkaStatus.CONSUMER_HEALTH;
    }

}
