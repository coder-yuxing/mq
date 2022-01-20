package com.yuxing.rocketmq.deliverymethods;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * rocketmq 各种消息投递方式
 *
 * @author yuxing
 * @since 2022/1/19
 */
public class RocketmqDeliveryMethodsTest {

    private DefaultMQProducer producer;
    private final String topic = "myTopic001";

    @Before
    public void setUp() throws Exception {
        // 指定生产组名为my-producer
        producer = new DefaultMQProducer("my-producer");
        // 配置nameserver地址
        String nameserver = "127.0.0.1:9876";
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
    public void testSendOneMsg() throws Exception {
        Message msg = new Message(topic, "hello world".getBytes());
        // 发送消息到mq，同步的
        SendResult result = producer.send(msg);
        System.out.println("发送消息成功！result is : " + result);
    }

    /**
     * 同步发送多条消息(批量发送)
     */
    @Test
    public void testSendMultiMsg() throws Exception {
        List<Message> messages = Arrays.asList(
                new Message(topic, "hello world1".getBytes()),
                new Message(topic, "hello world2".getBytes()),
                new Message(topic, "hello world3".getBytes())
        );
        // 批量发送的api的也是send()，只是他的重载方法支持List<Message>，同样是同步发送。
        SendResult result = producer.send(messages);
        System.out.println("发送消息成功！result is : " + result);
    }

    /**
     * 批量发送的topic必须是同一个，如果message对象指定不同的topic，那么批量发送的时候会报错
     */
    @Test(expected = MQClientException.class)
    public void testSendMultiMsgIfTopicIsNotSame() throws Exception {
        String topic2 = "myTopic002";
        List<Message> messages = Arrays.asList(
                new Message(topic, "hello world4".getBytes()),
                new Message(topic2, "hello world5".getBytes())
        );
        // 批量发送的api的也是send()，只是他的重载方法支持List<Message>，同样是同步发送。
        SendResult result = producer.send(messages);
        System.out.println("发送消息成功！result is : " + result);
    }

    /**
     * 异步发送消息
     */
    @Test
    public void testAsyncSendMsg() throws Exception{
        //  创建消息对象，topic为：myTopic001，消息内容为：hello world async
        Message msg = new Message(topic, "hello world async".getBytes());
        //  进行异步发送，通过SendCallback接口来得知发送的结果
        producer.send(msg, new SendCallback() {
            //  发送成功的回调接口
            @Override
            public void onSuccess(SendResult sendResult) {
                System.out.println("发送消息成功！result is : " + sendResult);
            }
            // 发送失败的回调接口
            @Override
            public  void  onException(Throwable throwable)  {
                throwable.printStackTrace();
                System.out.println("发送消息失败！result is : " + throwable.getMessage());
            }
        });

        Thread.sleep(2000);
    }

    /**
     * sendOneway() 方法
     * 效率最高 oneway不关心是否发送成功，我就投递一下我就不管了。所以返回是void
     */
    @Test
    public void testSendOneWay() throws Exception {
        // 创建消息对象，topic为：myTopic001，消息内容为：hello world oneway
        Message msg = new Message(topic, "hello world oneway".getBytes());
        producer.sendOneway(msg);
        System.out.println("投递消息成功！注意这里是投递成功，而不是发送消息成功哦！因为我sendOneway也不知道到底成没成功，我没返回值的。");
    }

}
