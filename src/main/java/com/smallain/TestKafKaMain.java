package com.smallain;
import com.smallain.kafkaclient.KafKaProducerFactory;
public class TestKafKaMain {
    public static void main(String[] args){
        String kfkServers = "iz2zea86z2leonw09hpjijz:9092,iz2zea86z2leonw09hpjimz:9092,iz2zea86z2leonw09hpjilz:9092,iz2zea86z2leonw09hpjikz:9092";
        String clientId = "TestProducer";

        //KafKaProducerFactory kafkaproducers = new KafKaProducerFactory(kfkServers,clientId);
       // kafkaproducers.pushKafKa("testkafka",1,"hahahahahha");

    }
}
