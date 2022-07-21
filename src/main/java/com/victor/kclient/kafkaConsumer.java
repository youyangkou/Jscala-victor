package com.victor.kclient;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Gerry
 * @date 2022年06月02日 3:21 下午
 */
public class kafkaConsumer implements Runnable {

    private List<String> topic_list;
    private String groupid;
    private String bootstrap;
    private ExecutorService executorPool;

    public kafkaConsumer(List<String> topic_list, String groupid, String bootstrap) {
        this.topic_list = topic_list;
        this.groupid = groupid;
        this.bootstrap = bootstrap;
    }

    public Properties setProperties(String groupid, String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        //每个消费者分配独立的组号
        props.put("group.id", groupid);
        //props.put("client.id",groupid);
        //如果value合法，则自动提交偏移量
        props.put("enable.auto.commit", "true");
        //设置多久一次更新被消费的偏移量
        props.put("auto.commit.interval.ms", "1000");
        //offset提交尝试最大次数
        props.put("offsets.commit.max.retries", "5");
        //无记录的话从头开始消费
//        props.put("auto.offset.reset","earliest");
        //无记录的话从最新开始消费
        props.put("auto.offset.reset", "latest");
        props.put("max.poll.records", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Override
    public void run() {
        //1.创建线程池
        this.executorPool = Executors.newFixedThreadPool(1);
        //创建指定consumer并执行
        executorPool.submit(new RealConsumer(setProperties(groupid, bootstrap), topic_list));
    }
}

/**
 * 执行consumer任务处理逻辑
 */
class RealConsumer implements Runnable {
    private Properties prop;
    private List<String> topic_list;

    private static final Logger logService = LogManager.getLogger(RealConsumer.class);

    public RealConsumer(Properties prop, List<String> topic_list) {
        this.prop = prop;
        this.topic_list = topic_list;
    }

    /**
     * 简单的方式订阅consumer并且并返回指定consumer对象
     *
     * @param prop
     * @param topic_list
     * @return
     */
    private Consumer ConsumerCreater(Properties prop, List<String> topic_list) {
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
        consumer.subscribe(topic_list);
        return consumer;
    }

    @Override
    public void run() {
        Consumer consumer = ConsumerCreater(prop, topic_list);
        String sout = null;
        int j = 0;
        int sleep = 10;
        int i = 0;
        while (true) {
            try {
                ConsumerRecords<String, String> recodes = consumer.poll(10);
                if (recodes.isEmpty()) {
                    Thread.sleep(sleep * j);
                    if (j >= 60) {
                        j = 60;
                    } else {
                        j++;
                    }
                    logService.info("没数据");
                    continue;
                }
                ;
                for (ConsumerRecord<String, String> record : recodes) {
                    if (record.value() == null || record.value().trim().length() == 0) {
                        System.out.println("offset: " + record);
                        continue;
                    }
                    String message = record.value();
                    //打印value的值
                    System.out.println(message);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

