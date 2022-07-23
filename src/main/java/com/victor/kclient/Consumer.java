package com.victor.kclient;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Gerry
 * @date 2022年06月02日 3:23 下午
 */
public class Consumer {
    public static void main(String[] args) {
        String bootstrap = "host.docker.internal:9092";//docker kafka
        String groupId = "flink_test";
        List<String> topic_list =new ArrayList<String>();
        topic_list.add("test-topic");//docker kafka topic
        kafkaConsumer kafkaConsumer=new kafkaConsumer(topic_list,groupId,bootstrap);
        new Thread(kafkaConsumer).start();
    }
}
