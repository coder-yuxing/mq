package com.yuxing.rocketmq.sequentialmessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * 顺序性消息
 * mq 消息存储在消息队列queue中，单个queue是能够保证消息的有序性的，问题在于每个topic有多个queue(这么设计的好处在于天然支持集群和负载均衡)
 * 当海量消息发送至topic后，会均匀分配到每个queue上，而消费时无法确定先消费那个queue，后消费哪个queue, 这就导致了无序消费
 *
 * rocketmq解决顺序消费的方案是，将顺序消费的消息都发送到同一个queue中，即在发送消息时指定要发送到topic的那个queue中
 * {@link org.apache.rocketmq.client.producer.MessageQueueSelector}
 *
 * @author yuxing
 * @since 2022/1/20
 */
public class SequentialMessageTest {

    private DefaultMQProducer producer;
    private final String nameserver = "127.0.0.1:9876";
    private final String topic = "order-topic";

    @Before
    public void setUp() throws Exception {
        // 指定生产组名为my-producer
        producer = new DefaultMQProducer("my-producer");
        // 配置nameserver地址
        producer.setNamesrvAddr(nameserver);
        // 启动Producer
        producer.start();
    }

    @After
    public void tearDown() {
        producer.shutdown();
    }

    /**
     * 发送顺序消息
     */
    @Test
    public void testSendSequentialMessage() throws Exception {
        for (int i = 0; i < 10; i++) {
            Message message = new Message(topic, ("hello world " + i).getBytes());
            producer.send(message, (list, message1, o) -> list.get(0), 0, 2000);
        }
    }

    @Test
    public void consumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("my-consumer");
        consumer.setNamesrvAddr(nameserver);
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerOrderly) (messages, context) -> {
            for (MessageExt msg : messages) {
                System.out.println(new String(msg.getBody()) + " Thread:" + Thread.currentThread().getName() + " queueId:" + msg.getQueueId());
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });

        consumer.start();
        System.out.println("Consumer start...");

        Thread.sleep(Integer.MAX_VALUE);
    }
}
