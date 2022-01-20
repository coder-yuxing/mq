package com.yuxing.rocketmq.transactionmessage;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事务消息 <br/>
 * rocketmq 采用2pc 思想实现了提交事务消息，同时增加了一个补偿逻辑用于处理二阶段超时或失败的消息 <br/>
 * <img src="https://github.com/apache/rocketmq/blob/master/docs/cn/image/rocketmq_design_10.png" />
 * https://github.com/apache/rocketmq/blob/master/docs/cn/design.md
 *
 * @author yuxing
 * @since 2022/1/20
 */
public class TransactionMessageTest {

    private final String nameserver = "127.0.0.1:9876";
    private final String topic = "transaction-topic";

    /**
     * 发送事务消息
     */
    @Test
    public void testSendTransactionMessage() throws Exception {

        // 指定生产组名为my-producer
        TransactionMQProducer producer = new TransactionMQProducer("my-transaction-producer");
        // 配置nameserver地址
        producer.setNamesrvAddr(nameserver);
        // 自定义线程池,执行事务操作
        // ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 50, 10L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(20), (Runnable r) -> new Thread("Order Transaction Massage Thread"));
        // producer.setExecutorService(executor);
        // 设置事物消息监听器
        producer.setTransactionListener(new CustomTransactionListener());
        // 启动Producer
        producer.start();

        for (int i = 0; i < 10; i++) {
            String orderId = UUID.randomUUID().toString();
            Message message = new Message(topic,"", orderId, ("下单， orderId=" + orderId).getBytes());
            producer.sendMessageInTransaction(message, orderId);
        }

        Thread.sleep(Integer.MAX_VALUE);
    }

    @Test
    public void consumer() throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setNamesrvAddr(nameserver);
        consumer.setConsumerGroup("my-transaction-consumer");
        consumer.subscribe(topic, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, consumeConcurrentlyContext) -> {
            for (MessageExt msg : messages) {
                String orderId = msg.getKeys();
                System.err.println("监听到下单消息， orderId: " + orderId + "; 执行商品减库存操作");
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        consumer.start();
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 事务消息监听器：主要用于进行本地事务执行和事务状态回查
     *
     */
    public static class CustomTransactionListener implements TransactionListener {

        private static final Map<String, Boolean> results = new ConcurrentHashMap<>();

        @Override
        public LocalTransactionState executeLocalTransaction(Message message, Object arg) {
            String orderId = (String) arg;
            // 记录本地事务执行结果
            boolean isSuccess = this.persistTransactionResult(orderId);
            return isSuccess ? LocalTransactionState.COMMIT_MESSAGE : LocalTransactionState.ROLLBACK_MESSAGE;
        }

        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
            String orderId = messageExt.getKeys();

            System.err.println("执行事务消息回查，orderId: " + orderId);
            return Boolean.TRUE.equals(results.get(orderId)) ? LocalTransactionState.COMMIT_MESSAGE : LocalTransactionState.ROLLBACK_MESSAGE;
        }

        private boolean persistTransactionResult(String orderId) {
            boolean success = Math.abs(Objects.hash(orderId)) % 2 == 0;
            results.put(orderId, success);
            System.err.println("开始下单，订单号：" + orderId + "; 本地事务执行结果：" + (success ? "成功" : "失败"));
            return success;
        }
    }


}
