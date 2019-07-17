package com.han.kafka.consumer.monitor;



import com.han.kafka.consumer.Utils.KafkaOptionUtil;
import com.han.kafka.consumer.api.ConsumerOffsetInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author hanlin01
 * @date :2019/5/17
 * @desc: kafka自动扩容分区的任务, 扩展分区的策略
 * 1.默认监听所有分区的扩容,也可以注册监听指定的topic
 * 2.执行任务时长和扩容时机的定义
 * 3.扩容机制默认是双倍扩容,可以指定分区上限默认为1000是上限
 * 4.可以指定分区的消费速率，默认为2000/s
 * 5.计算扩容的方式是:计算出每小段时间内的offset的变化，得出一个时间序列的offset变化的数组，
 * 然后去除最大和最小值进行平均值计算，将计算结果和perSecondLimit进行比较
 */
@Slf4j
public class DefaultKafkaPartitionExpandStrategy extends AbstractKafkaPartitionExpandStrategy {

    //配置任务多长时间执行一次,单位秒
    private int priod;

    //执行多少次任务后开始计算扩容分区的操作
    private int count;
    //扩容条件
    private int perSecondLimit;

    private int partitionLimit;

    private Set<String> filterTopics;

    private ExecutorService executorService = null;

    public DefaultKafkaPartitionExpandStrategy(int priod, int count, int perSecondLimit, int partitionLimit, Set<String> filterTopics) {
        this.priod = priod;
        if (count < 4) {
            throw new IllegalArgumentException("The number of times to obtain partition information must be greater than 3");
        }
        this.count = count;
        this.perSecondLimit = perSecondLimit;
        this.partitionLimit = partitionLimit;
        if (null == filterTopics) {
            filterTopics = new HashSet<String>();
        }
        filterTopics.add("__consumer_offsets");
        this.filterTopics = filterTopics;
    }

    @Override
    public void expand() {
        log.info("Start execute expand partition task datetime={}", new Date());
        //先读取相关的partition信息
        getExpandParitionInfos();

        Iterator<Map.Entry<String, Map<String, Map<Integer, List<Long>>>>> it =null;
        while (it.hasNext()) {
            Map.Entry<String, Map<String, Map<Integer, List<Long>>>> en = it.next();
            String topic = en.getKey();
            Map<String, Map<Integer, List<Long>>> groupPartitionsMap = en.getValue();
            Runnable r = createExpandRunnable(topic, groupPartitionsMap);
            boolean asyn =true;//是否异步执行扩容任务
            if (asyn) {
                if (null == executorService) {
                    executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
                }
                executorService.submit(r);
            } else {
                r.run();
            }
        }
    }

    public Runnable createExpandRunnable(String topic, Map<String, Map<Integer, List<Long>>> groupPartitionsMap) {

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                Iterator<Map.Entry<String, Map<Integer, List<Long>>>> it = groupPartitionsMap.entrySet().iterator();
                boolean hadExpand = false;//在本次计算周期中是否已经扩容了一次
                while (it.hasNext()) {
                    Map.Entry<String, Map<Integer, List<Long>>> en = it.next();
                    Map<Integer, List<Long>> partitionOffsetMap = en.getValue();
                    String groupName = en.getKey();

                    //计算周期数据状态检查
                    boolean checkStat = checkCountStat(topic, groupName, partitionOffsetMap);
                    if (!checkStat) {

                        continue;
                    }
                    log.info("calculate split partition topic={},group={},data={}", new Object[]{topic, groupName, partitionOffsetMap.toString()});

                    Long groupTotalPartitionEPS = 0L;
                    for (List list : partitionOffsetMap.values()) {

                        long perPartitionEps = caclPartitionEPS(list);
                        groupTotalPartitionEPS += perPartitionEps;
                    }

                    //int numPartitions = TopicGroupOffsetCache.getPartitionNumByTopic(topic);
                    int numPartitions=0;
                    int addNumPartitions = getAddNumParitions(topic, groupName, groupTotalPartitionEPS, numPartitions);
                    if (addNumPartitions > 0 && !hadExpand) {
                        doExpand(topic, addNumPartitions);//扩容请求
                        hadExpand = true;

                        log.info("Expand topic partition sunccess,topic={},addNumPartition={}", topic, addNumPartitions);
                    }
                  //  TopicGroupOffsetCache.clearOffsetInfoByToic(topic, groupName); //删除计算周期内的缓存数据

                }
            }
        };
        return runnable;
    }

    private boolean checkCountStat(String topic, String group, Map<Integer, List<Long>> partitionOffsetMap) {
        Iterator<Map.Entry<Integer, List<Long>>> it = partitionOffsetMap.entrySet().iterator();
        int last = -1;

        while (it.hasNext()) {
            List<Long> list = it.next().getValue();
            int size = list.size();

            if (last == -1) {
                last = size;
            }
            if (size > count || last != size) {//脏数据清理掉
                log.warn("check partition data exception,topic={},group={},data={}", new Object[]{topic, group, partitionOffsetMap.toString()});
                //TopicGroupOffsetCache.clearOffsetInfoByToic(topic, group);
                return false;

            }
        }
        if (last < count) {//这个是还没到计算周期,不做扩容计算
            return false;
        }
        return true;
    }


    /**
     * @param groupTotalPartitionEPS 某个topic下的一个分组的的总EPS
     * @param partitionNum
     * @return
     */
    private int getAddNumParitions(String topic, String groupName, long groupTotalPartitionEPS, int partitionNum) {

        double avgeParitionEPS = groupTotalPartitionEPS / partitionNum;
        Double multiple = Math.ceil(avgeParitionEPS / perSecondLimit);
        log.info("parmeters topic={},group={}, groupTotalPartitionEPS={},partitionNum={},avgeParitionEPS={},multiple={}", new Object[]{
                topic, groupName, groupTotalPartitionEPS, partitionNum, avgeParitionEPS, multiple});

        int addNumPartition = 0;
        if (multiple > 1.0) {
            //扩容后的总分区数
            int newTotalPartitions = partitionNum * (multiple.intValue());

            //如果扩容后总分区数大于分区数上限，则取上限分区数
            if (newTotalPartitions > this.partitionLimit) {
                newTotalPartitions = this.partitionLimit;
            }
            //去除原有的分区数，就是需要增加的分区数
            addNumPartition = newTotalPartitions;
            if(newTotalPartitions<partitionNum){
                return 0;
            }
            log.info("caculate topic={},group={}, totalPartitionEps={} ,avgeParitionEPS={},addNumPartition={}",
                    new Object[]{topic, groupName, groupTotalPartitionEPS, avgeParitionEPS, addNumPartition});
        }


        return addNumPartition;
    }

    /**
     * 计算每个分区的EPS，去掉最大值、最小值求平均
     *
     * @param partitions
     * @return
     */
    private long caclPartitionEPS(List<Long> partitions) {
        int size = partitions.size();
        Long arr[] = new Long[size - 1];
        for (int i = 0; i < size; i++) {
            if (i < size - 1) {

                arr[i] = partitions.get(i + 1) - partitions.get(i);
                if (arr[i] < 0) {//发生这种情况应该是consumeOffset被重置了,先打印日志记录一下
                    log.warn("Offset difference is less than 0...");
                }
            }
        }

        Arrays.sort(arr);
        List<Long> list = Arrays.asList(arr);
        list = list.subList(1, list.size() - 1);//排序后，去掉最大和最小值

        long sum = 0;
        for (Long consumSize : list) {

            sum += consumSize;
        }
        long avgConsumeSize = sum / ((priod) * (list.size()));//求这段时间内的平均值

        return avgConsumeSize;
    }


    private void getExpandParitionInfos() {
        long start = System.currentTimeMillis();
        List<ConsumerOffsetInfo> consumerOffsetInfoList = KafkaOptionUtil.getAllTopicConsumerOffsetNew();
        long end = System.currentTimeMillis();
        log.info("Get the partition info from kafka tims={}ms", end - start);
        if (log.isDebugEnabled()) {
            log.debug("Get the partition info from kafka data={}", consumerOffsetInfoList.toString());
        }

        for (ConsumerOffsetInfo consumerOffsetInfo : consumerOffsetInfoList) {
            String topic = consumerOffsetInfo.getTopic();
            //过滤不需要监听扩容分区的topic,默认监控所有topic
            if (null != filterTopics && !this.filterTopics.isEmpty()) {
                if (filterTopics.contains(topic)) {
                    continue;
                }
            }

           // TopicGroupOffsetCache.add(consumerOffsetInfo);
        }

    }

}
