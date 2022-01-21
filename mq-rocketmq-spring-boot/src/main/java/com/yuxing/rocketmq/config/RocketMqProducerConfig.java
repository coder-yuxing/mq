package com.yuxing.rocketmq.config;

import lombok.AllArgsConstructor;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;

/**
 * rocketmq 生产者配置
 *
 * @author yuxing
 * @since 2022/1/20
 */
@Configuration
public class RocketMqProducerConfig {

    /**
     * TODO
     */
    private final TransactionListener transactionListener = new TransactionListener() {
        @Override
        public LocalTransactionState executeLocalTransaction(Message message, Object o) {
            return null;
        }

        @Override
        public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
            return null;
        }
    };

    @Value("${rocketmq.name-server}")
    private String nameserver;

    @Bean
    public RocketMQTemplate transactionRocketMqTemplate(TransactionMQProducer transactionMQProducer) {
        RocketMQTemplate rocketMQTemplate = new RocketMQTemplate();
        rocketMQTemplate.setProducer(transactionMQProducer);
        return rocketMQTemplate;
    }

    @Bean(name = "transactionMQProducer", initMethod = "start", destroyMethod = "shutdown")
    public TransactionMQProducer transactionMQProducer(ExecutorService rocketmqTransPool) {
        // 指定生产组名为my-producer
        TransactionMQProducer producer = new TransactionMQProducer("my-transaction-producer");
        // 配置nameserver地址
        producer.setNamesrvAddr(this.nameserver);
        // 自定义线程池,执行事务操作
        producer.setExecutorService(rocketmqTransPool);
        // 设置事物消息监听器
        producer.setTransactionListener(transactionListener);
        return producer;
    }
}
