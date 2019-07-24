package com.rrym;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;


import java.util.List;


public class Consumer {
    public static void main(String[] args) throws MQClientException {
        // 使用指定的使用者组名称进行实例化
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("rmq-group");
        // 指定名称服务器地址。
        consumer.setNamesrvAddr("172.16.157.128:9876");
        // 订阅一个要消费实例名称。
        consumer.setInstanceName("consumer");
        // 订阅一个要消费的主题。
        consumer.subscribe("TopicA-test", "TagA");
        //注册回调以在从代理获取的消息到达时执行。
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(
                    List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动消费者实例。
        consumer.start();

        System.out.println("Consumer Started.");
    }
}
