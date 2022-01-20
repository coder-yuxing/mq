package com.yuxing.rocketmq.messagemodel;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.junit.Before;
import org.junit.Test;

/**
 * rocketmq 消费模式测试
 *
 * @author yuxing
 * @since 2022/1/20
 */
public class RocketmqMessageModelTest {

    private final String nameserver = "127.0.0.1:9876";
    private final String topic = "myTopic001";

    private DefaultMQPushConsumer consumer;

    @Before
    public void setUp() throws Exception {
        consumer = new DefaultMQPushConsumer("my-consumer");
        // 配置nameserver地址
        consumer.setNamesrvAddr(nameserver);
        // 订阅topic：myTopic001 下的全部消息（因为是*，*指定的是tag标签，代表全部消息，不进行任何过滤）
        consumer.subscribe(topic, "*");
    }

    /**
     * 集群模式测试：
     * 集群模式，producer生成一条消息后，最终只能由一个消费者消费
     */
    @Test
    @SuppressWarnings("Duplicates")
    public void testClusteringModel() throws Exception {
        // 设置消费模式为集群模式， 该模式为默认模式
        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 注册监听器，进行消息消息。
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, consumeConcurrentlyContext) -> {
            for (MessageExt msg : messages) {
                String str = new String(msg.getBody());
                //  输出消息内容
                System.out.println(str);
            }
            //  默认情况下，这条消息只会被一个consumer消费，这叫点对点消费模式。也就是集群模式。
            //  ack确认
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        //  启动消费者
        consumer.start();
        System.out.println("Consumer  start");

        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 测试广播模式
     * 该模式下，producer生成一条消息后，会被所有订阅该topic的消费者消费一次
     * producer生成的消息仅会被当前广播的消费者消费一次，后续加入的消费者不会消费这条消息
     */
    @Test
    @SuppressWarnings("Duplicates")
    public void testBroadcasting() throws Exception {
        // 设置消费模式为广播模式，
        consumer.setMessageModel(MessageModel.BROADCASTING);
        // 注册监听器，进行消息消息。
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, consumeConcurrentlyContext) -> {
            for (MessageExt msg : messages) {
                //  输出消息内容
                System.out.println(new String(msg.getBody()));
            }
            //  ack确认
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        //  启动消费者
        consumer.start();
        System.out.println("Consumer  start");

        Thread.sleep(Integer.MAX_VALUE);
    }
}
