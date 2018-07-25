package com.smallain.kafkaclient;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;


public class KafkaDao {

    private String brokerUrl;

    Properties properties = new Properties();

    KafkaDao(String brokerUrls) {
        this.brokerUrl = brokerUrls;

        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
    }


    public void createTopics(String new_topic, int numPartitions, short replicationFactors) {
        AdminClient adminClient = AdminClient.create(properties);
        NewTopic newTopic = new NewTopic(new_topic, numPartitions, replicationFactors);
        Collection<NewTopic> newTopicList = new ArrayList<>();
        newTopicList.add(newTopic);
        adminClient.createTopics(newTopicList);
        adminClient.close();
    }


    public Boolean checkTopics(String topic_name) {
        AdminClient adminClient = AdminClient.create(properties);
        Boolean existFlag;
        ListTopicsResult ltr = adminClient.listTopics();

        try {
            Set<String> topicNames = ltr.names().get();
            existFlag = topicNames.contains(topic_name);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            existFlag = false;
        }
        adminClient.close();
        return existFlag;

    }


//    public Boolean checkTopics(String topic_name, AdminClient adminClient) {
//
//        Boolean existFlag;
//        ListTopicsResult ltr = adminClient.listTopics();
//
//        try {
//            Set<String> topicNames = ltr.names().get();
//            existFlag = topicNames.contains(topic_name);
//        } catch (InterruptedException | ExecutionException e) {
//            e.printStackTrace();
//            existFlag = false;
//        }
//
//        return existFlag;
//
//    }

    public void deleteTopics(String topic_name) {
        AdminClient adminClient = AdminClient.create(properties);
        Collection topicList = new ArrayList<>();
        topicList.add(topic_name);
        adminClient.deleteTopics(topicList);
        adminClient.close();

    }

}
