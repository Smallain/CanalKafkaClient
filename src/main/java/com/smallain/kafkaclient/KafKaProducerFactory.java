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

    public void pushKafKa(String topicKfK, int messageNoKfK, String messageStrKfK) {
        KafkaProducer producer = new KafkaProducer(props);
        String topic = topicKfK;
        int messageNo = messageNoKfK;
        String messageStr = messageStrKfK;
        long startTime = System.currentTimeMillis();

        producer.send(new ProducerRecord(topic, messageNo, messageStr), new KfKCallBack(startTime, messageNo, messageStr));
        producer.close();

    }

}

