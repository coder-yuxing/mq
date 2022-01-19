package com.yuxing.helloworld;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;

/**
 * rocketmq 测试类
 * {@link RocketMqTest#producer()} 实现基于rocketmq发送消息
 * {@link RocketMqTest#consumer()} 实现基于rocketmq消费消息
 *
 * @author yuxing
 * @since 2022/1/19
 */
public class RocketMqTest {

    private final String nameserver = "127.0.0.1:9876";
    private final String topic = "myTopic001";

    @Test
    public void producer() throws Exception {
        // 指定生产组名为my-producer
        DefaultMQProducer producer = new DefaultMQProducer("my-producer");
        // 配置 namesever 地址
        producer.setNamesrvAddr(nameserver);
        // 启动Producer
        producer.start();
        // 创建消息对象，topic为：myTopic001，消息内容为：hello world
        Message msg = new Message(topic, "hello world".getBytes());
        // 发送消息到mq，同步的
        SendResult result = producer.send(msg);
        System.out.println("发送消息成功！result is : " + result);
        // 关闭Producer
        producer.shutdown();
        System.out.println("生产者 shutdown！");
    }

    @Test
    public void consumer() throws Exception {
        //  指定消费组名为my-consumer
        DefaultMQPushConsumer consumer  =  new DefaultMQPushConsumer("my-consumer");
        //  配置nameserver地址
        consumer.setNamesrvAddr(nameserver);
        //  订阅topic：myTopic001 下的全部消息（因为是*，*指定的是tag标签，代表全部消息，不进行任何过滤）
        consumer.subscribe(topic,  "*");
        //  注册监听器，进行消息消息。
        consumer.registerMessageListener(new  MessageListenerConcurrently()  {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext)  {
                for  (MessageExt  msg  :  msgs)  {
                    String  str  =  new  String(msg.getBody());
                    //  输出消息内容
                    System.out.println(str);
                }
                //  默认情况下，这条消息只会被一个consumer消费，这叫点对点消费模式。也就是集群模式。
                //  ack确认
                return  ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //  启动消费者
        consumer.start();
        System.out.println("Consumer  start");

        Thread.sleep(Integer.MAX_VALUE);
    }
}
