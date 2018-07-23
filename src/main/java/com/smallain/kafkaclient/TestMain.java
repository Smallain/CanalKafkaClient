package com.smallain.kafkaclient;

public class TestMain {
    public static void main(String args[]) {
        KafkaDao kfd = new KafkaDao("iz2zea86z2leonw09hpjijz:9092,iz2zea86z2leonw09hpjimz:9092,iz2zea86z2leonw09hpjilz:9092,iz2zea86z2leonw09hpjikz:9092");
        kfd.createTopics("wuyuhang_test", 4, (short) 3);


    }
}
