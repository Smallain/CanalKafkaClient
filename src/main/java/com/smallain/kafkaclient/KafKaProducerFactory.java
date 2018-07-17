package com.smallain.kafkaclient;

import org.apache.kafka.clients.producer.*;

import java.util.*;

public class KafKaProducerFactory {
    Properties props = new Properties();

    public KafKaProducerFactory(String kfkServers, String clientId) {
        this.props.put("bootstrap.servers", kfkServers);
        this.props.put("client.id", clientId);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("retries", "5");
    }

    //TODO KAFKA TOPIC 设计与 Partition设计,针对不同的canal放到不同的topick partition中
    //String topic, Integer partition, K key, V value

    public void pushKafKa(String topicKfK, int messageNoKfK, String messageStrKfK) {
        KafkaProducer producer = new KafkaProducer(props);
        String topic = topicKfK;
        int messageNo = messageNoKfK;
        String messageStr = messageStrKfK;
        long startTime = System.currentTimeMillis();

        //异步调用
        producer.send(new ProducerRecord(topic, messageNo, messageStr), new KfKCallBack(startTime, messageNo, messageStr));
        producer.close();

    }

}

