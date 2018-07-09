package kafkaclient;

import org.apache.kafka.clients.producer.*;

import java.util.*;
import java.util.concurrent.*;

public class ProducerModule {
    public static void main(String[] args) {
        //消息的发送方式：异步发送还是同步发送
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        Properties props = new Properties();
        //Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "192.168.31.101:9092,192.168.31.102:9092,192.168.31.103:9092");
        props.put("client.id", "DemoProducer");//客户端ID

        //消息key和value都是字节数组，为了将Java对象转化为字节数组，可以配置key.serializer和value.serializer两个序列化器完成转换
        //serializer用来将string转化为字节数组
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(props);//生产者核心类
        String topic = "testkafka";//向指定的topic发送消息

        int messageNo = 1;//消息的Key

        while (true) {
            String messageStr = "Message_" + messageNo;//消息的value
            long startTime = System.currentTimeMillis();
            if (isAsync) {
                //异步发送消息
                //第一个参数是ProducerRecord类型的对象，封装了目标topic、消息的key、消息的value
                //第二个参数是一个CallBack对象，对生产者接收到kafka发来的ack确认消息的时候，会调用此CallBack对象onCompletion（）方法，实现回调功能
                producer.send(new ProducerRecord(topic, messageNo, messageStr), new DemoCallBack(startTime, messageNo, messageStr));
            } else {
                //同步发送消息
                try {
                    //KafkaProducer.send()方法的返回值类型是Future<RecordMetadata>
                    //这里通过Future.get()方法，阻塞当前线程等待Kafka服务端ACK响应
                    producer.send(new ProducerRecord(topic, messageNo, messageStr)).get();
                    System.out.println("Send message:(" + messageNo + "," + messageStr + ")");
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;//递增消息的key
        }
    }
}

class DemoCallBack implements Callback {
    private final long startTime;//开始发送消息的时间戳
    private final int key;//消息的key
    private final String message;//消息的value

    public DemoCallBack(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }


    /**
     * 生产者成功发送消息,接收到Kafka服务端发来的ACK确认消息后，会调用此回调函数
     *
     * @param metadata  生产者发送的消息的愿数据，如果发送过程中出现异常，此参数为null
     * @param exception 发送过程中出现的异常，如果发送成功，则此参数为null
     */
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if (metadata != null) {
            //RecordMetadata中包含了分区的信息、offset的信息等
            System.out.println("message(" + key + "," + message + ") sent " +
                    "to partition(" + metadata.partition() + ")" + "offset(" + metadata.offset() + ") in " + elapsedTime + "ms");
        } else {
            exception.printStackTrace();
        }
    }
    //回调对象
}