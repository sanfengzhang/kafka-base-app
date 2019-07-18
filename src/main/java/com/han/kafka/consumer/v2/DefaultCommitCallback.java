package com.han.kafka.consumer.v2;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;

/**
 * @author: Hanl
 * @date :2019/7/18
 * @desc:
 */
@Slf4j
public class DefaultCommitCallback implements CommitCallback {

    private long timeout = 10L;

    private boolean failedCommitIf = false;

    @Override
    public void onSuccess(final ConsumerTask consumerTask, Map<TopicPartition, OffsetAndMetadata> offsets) {
        //FIXME 比如在提交offset异常的时候该如何处理，保障系统的可靠性和一致性
        //程序执行到这里的时候表明，业务处理应该是处理成功的但提交offset失败，可能因为网络的原因或者其他的问题
        //可以尝试重试机制去保障、如果仍然不行？，我们可以在consume poll到records的时候将offset信息放在内存
        try {
            consumerTask.consumer.commitSync(offsets, Duration.ofSeconds(timeout));
        } catch (Exception e) {
            log.error("commit offset exception e={}", e);
        }
    }

    /**
     * FIXME consumer.commitSync这个方法的实现是有重试机制的，会再timeout时间内一直尝试提交直到成功
     * 所以次逻辑内可以不需要实现重试，只要适当设置commitTimeout的大小即可
     * 客户端配置可以加入：retry.backoff.ms这个指定consumer在尝试重试提交时候可以延迟多少ms之后进行再次提交
     */
    @Override
    public void onSuccess(final ConsumerTask consumerTask) {
        try {
            consumerTask.consumer.commitSync(Duration.ofSeconds(timeout));
        } catch (Exception e) {
            log.error("commit offset exception e={}", e);
            if (failedCommitIf) {
                 //提交失败，可以采取异步的消息机制将该异常提交给消息通知系统

                //end暂停消费线程
                consumerTask.pauseConsumer();
            }
        }
    }
}
