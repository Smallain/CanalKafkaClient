package com.smallain.kafkaclient;

import org.apache.kafka.clients.producer.*;

import java.util.*;

public class KafKaProducerFactory {
    Properties props = new Properties();

    public KafKaProducerFactory(String kfkServers, String clientId) {
        this.props.put("bootstrap.servers", kfkServers);
        this.props.put("client.id", clientId);
        //org.apache.kafka.common.serialization.IntegerSerializer
        this.props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.props.put("acks", "all");
        this.props.put("retries", "5");
    }

    //TODO KAFKA TOPIC 设计与 Partition设计,针对不同的canal放到不同的topick partition中
    //TODO 检查topic是否存在，不存在创建，存在插入数据
    //TODO 自定义分区器，对city_id进行散列处理，然后对分区数量进行模余操作，保证相同的city_id会被分配到相同的分区中。
    //String topic, Integer partition, K key, V value

    public void pushKafKa(String topicKfK, String partitionKafka, String messageNoKfK, String messageStrKfK, String serverskfk) {
        KafkaProducer producer = new KafkaProducer(props);
        String topic = topicKfK;
        //TODO messageNo    采用md5加密方式作为kafka数据存储的负载

        String messageStr = messageStrKfK;
        long startTime = System.currentTimeMillis();

        int kafkaPartition = Integer.valueOf(partitionKafka);
        //异步调用


        KafkaDao kfd = new KafkaDao(serverskfk);
        Boolean isTopicExist = kfd.checkTopics(topic);

        if (isTopicExist) {
            producer.send(new ProducerRecord(topic, kafkaPartition, messageNoKfK, messageStr), new KfKCallBack(startTime, messageNoKfK, messageStr));
        } else {
            kfd.createTopics(topic, 60, (short) 3);
            producer.send(new ProducerRecord(topic, kafkaPartition, messageNoKfK, messageStr), new KfKCallBack(startTime, messageNoKfK, messageStr));
        }


        producer.close();

    }

}

