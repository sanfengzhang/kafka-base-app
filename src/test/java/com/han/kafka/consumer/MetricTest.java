package com.han.kafka.consumer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;

/**
 * @author: Hanl
 * @date :2019/7/31
 * @desc:
 */
public class MetricTest {

    private MetricRegistry metricRegistry = new MetricRegistry();

    private Counter count = null;

    @Before
    public void setup() {

        count = metricRegistry.counter("count");
    }

    /**
     * 下面这单程序count不是线程安全的
     * @throws Exception
     */
    @Test
    public void testCounter() throws Exception {
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    System.out.println(metricRegistry.counter("count").getCount());
                    try {
                        Thread.sleep(1000l);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                }

            }
        }.start();

        new Thread() {
            @Override
            public void run() {

                while (count.getCount() < 10L) {

                    try {
                        Thread.sleep(400L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    count.inc();
                    System.out.println("1--"+metricRegistry.counter("count").getCount());
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {

                while (count.getCount() < 10L) {

                    try {
                        Thread.sleep(400L);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    count.inc();
                    System.out.println("2--"+metricRegistry.counter("count").getCount());
                }
            }
        }.start();

        System.in.read();
    }

}
