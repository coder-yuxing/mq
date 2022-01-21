package com.yuxing.rocketmq.config;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;

/**
 * 线程池配置
 *
 * @author yuxing
 * @since 2022/1/20
 */
@Configuration
public class ThreadPoolConfig {

    @Bean(name = "rocketmqTransPool")
    public ExecutorService rocketmqTransPool() {
        ThreadFactory threadFactory = new DefaultThreadFactory("rocketmq-trans-pool");
        int corePoolSize = Runtime.getRuntime().availableProcessors();
        int maximumPoolSize = (int) (corePoolSize / (1 - 0.8));
        return new ThreadPoolExecutor(
                corePoolSize,
                maximumPoolSize,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                threadFactory,
                new ThreadPoolExecutor.AbortPolicy()
        );
    }
}
