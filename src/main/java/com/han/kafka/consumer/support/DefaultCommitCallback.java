package com.han.kafka.consumer.support;

import com.han.kafka.common.CommitCallback;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;

/**
 * @author: Hanl
 * @date :2019/5/30
 * @desc:
 */
@Slf4j
public class DefaultCommitCallback implements CommitCallback {

    private long commitTimeout = 10L;

    private boolean pauseConsumerPoll = false;

    @Override
    public void onSuccess(final Consumer consumer, Map<TopicPartition, OffsetAndMetadata> offsets) {
        //FIXME 比如在提交offset异常的时候该如何处理，保障系统的可靠性和一致性
        //程序执行到这里的时候表明，业务处理应该是处理成功的但提交offset失败，可能因为网络的原因或者其他的问题
        //可以尝试重试机制去保障、如果仍然不行？，我们可以在consume poll到records的时候将offset信息放在内存
        try {
            consumer.commitSync(offsets, Duration.ofSeconds(commitTimeout));
        } catch (Exception e) {
            log.warn("commit offset exception e={}", e);
            int count = 0;
            boolean success = false;
            do {
                count++;
                try {
                    Thread.sleep(1000 * (count + 2));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                consumer.commitSync(offsets, Duration.ofSeconds(10));
                success = true;
            } while (count < 3 && !success);
            if (!success && pauseConsumerPoll) {
                //将consumer的状态切换为暂停消费
                consumer.paused();
            }
        }
    }

    @Override
    public void onSuccess(final Consumer consumer) {
        try {
            consumer.commitSync(Duration.ofSeconds(commitTimeout));
        } catch (Exception e) {
            log.warn("commit offset exception e={}", e);
            int count = 0;
            boolean success = false;
            do {
                count++;
                try {
                    Thread.sleep(1000 * (count + 2));
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
                consumer.commitSync(Duration.ofSeconds(10));
                success = true;
            } while (count < 3 && !success);
            if (!success && pauseConsumerPoll) {
                //将consumer的状态切换为暂停消费
                consumer.paused();
            }
        }
    }
}
