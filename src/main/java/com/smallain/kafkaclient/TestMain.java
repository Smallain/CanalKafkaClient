package com.smallain.kafkaclient;


import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.smallain.util.MD5Util.md5HashString;

public class TestMain {



    public static void main(String args[]) {
//        KafkaDao kfd = new KafkaDao("iz2zea86z2leonw09hpjijz:9092,iz2zea86z2leonw09hpjimz:9092,iz2zea86z2leonw09hpjilz:9092,iz2zea86z2leonw09hpjikz:9092");
//
//        Boolean flag = kfd.checkTopics("wuyuhang_canal_30");
//
//        if (flag) {
//            System.out.println(" hahahahahahaha   topic 已经存在了");
//
//        } else {
//            System.out.println("创建topic");
//            kfd.createTopics("wuyuhang_canal_30", 30, (short) 3);
//        }

        System.out.println(md5HashString("wuyuhang"));

    }
}
