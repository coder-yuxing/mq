package com.yuxing.rocketmq.tagandkey;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * rocketmq 消息 tags/keys
 * 订阅消息一致：
 *   订阅关系一致指的是同一个消费组(consumer group)下所有consumer实例所订阅的topic、tag必须完全一致。
 *   如果订阅关系不一致，消息消费的逻辑就会混乱，甚至导致消息丢失
 *   https://help.aliyun.com/document_detail/43523.html
 *
 * @author yuxing
 * @since 2022/1/20
 */
public class RocketMqTagAndKeyTest {

    private DefaultMQProducer producer;
    private final String nameserver = "127.0.0.1:9876";
    private final String topic = "myTopic001";

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
     * 同步发送一条消息
     */
    @Test
    public void testMessageTag() throws Exception {

        String tagFormat = "custom-tag-%s";
        for (int i = 0; i < 10; i++) {
            String tag;
            if (i % 2 == 0) {
                tag = String.format(tagFormat, "even");
            } else {
                tag = String.format(tagFormat, "odd");
            }
            Message msg = new Message(topic, tag, ("hello world " + i).getBytes());
            // 发送消息到mq，同步的
            SendResult result = producer.send(msg);
            System.out.println("发送消息成功！result is : " + result);
        }

    }

    /**
     * 消费tag标识为偶数的消息者
     */
    @Test
    @SuppressWarnings("Duplicates")
    public void testConsumerEven() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("my-consumer-1");
        consumer.setNamesrvAddr(nameserver);
        consumer.subscribe(topic, "custom-tag-even");
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, consumeConcurrentlyContext) -> {
            for (MessageExt msg : messages) {
                String str = new String(msg.getBody());
                System.out.println(str);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.println("Consumer  start");

        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 消费tag标识为奇数的消息者
     */
    @Test
    @SuppressWarnings("Duplicates")
    public void testConsumerOdd() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("my-consumer-2");
        consumer.setNamesrvAddr(nameserver);
        consumer.subscribe(topic, "custom-tag-odd");
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, consumeConcurrentlyContext) -> {
            for (MessageExt msg : messages) {
                String str = new String(msg.getBody());
                System.out.println(str);
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
        System.out.println("Consumer  start");

        Thread.sleep(Integer.MAX_VALUE);
    }

}
