package com.rrym;


import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;


import java.util.Date;


public class Producer {
    public static void main(String[] args) throws MQClientException {
        //使用生产者组名称进行实例化。
        DefaultMQProducer producer = new DefaultMQProducer("rmq-group");
        // 指定名称服务器地址。
        producer.setNamesrvAddr("172.16.157.128:9876");
        // 指定实例名称。
        producer.setInstanceName("producer");
        //启动实例
        producer.start();
        try {
            for (int i = 0; i < 10; i++) {
                Thread.sleep(1000);  //每秒发送一次MQ
                //创建消息实例，指定主题，标记和消息正文。
                Message msg = new Message("TopicA-test",// topic
                        "TagA",// tag
                        (new Date() + "Hello RocketMQ ,QuickStart" + i)
                                .getBytes()// body
                );
                //呼叫发送消息以向其中一个经纪人发送消息。
                SendResult sendResult = producer.send(msg);

                System.out.println(sendResult);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        //生产者实例不再使用后关闭。
        producer.shutdown();
    }

}
