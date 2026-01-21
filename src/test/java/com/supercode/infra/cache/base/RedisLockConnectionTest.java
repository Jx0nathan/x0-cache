package com.supercode.infra.cache.base;

import com.supercode.infra.cache.lock.RedisLock;
import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;

public class RedisLockConnectionTest {

    SupercodeRedisClient<String, String> redisClient;

    @BeforeEach
    public void before() {
        redisClient = new SupercodeRedisClient<>("tf-usa-dev-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com", 6379);
    }

    /**
     * 在dev环境测下来看。连接池的初始化基本耗时在34秒左右
     * first connection cost --：35405
     * first connection cost --：34884
     * first connection cost --：34537
     * first connection cost --：34223
     * first connection cost --：34054
     */
    @Test
    public void benchmarkRedisLockConnectionPool() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        int count = 0;
        for (int i = 0; i < 1000; i++) {
            Thread.sleep(100);
            int numIdle = redisClient.getPool().getNumIdle();
            if (numIdle > 0) {
                if (count == 1) {
                    long endTime = System.currentTimeMillis();
                    System.out.println("first connection cost --：" + (endTime - startTime));
                }
                System.out.println("numIdel--：" + numIdle);
                count++;
            }
        }
    }

    @SneakyThrows
    @Test
    public void benchmarkRedisLock() {
        SupercodeRedisClient<String, String> redisClient = new SupercodeRedisClient<>("tf-usa-dev-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com", 6379);
        RedisLock<String, String> redisLock = new RedisLock<>(redisClient);

        int threadCount = 1;
        Thread[] threads = new Thread[threadCount];
        CountDownLatch startLatch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    startLatch.countDown();
                    startLatch.await();

                    int count = 0;
                    while (count < 100) {
                        if (redisLock.tryRedLock("test-jd", 100000)) {
                            System.out.printf("%s acquired lock%n success", Thread.currentThread().getName());
                            redisLock.releaseRedLock("test-jd");
                            count++;
                            System.out.println("count : " + count);
                        } else {
                            System.out.printf("%s acquired lock%n fail", Thread.currentThread().getName());
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            });
        }

        for (Thread thread : threads) thread.start();
        for (Thread thread : threads) thread.join();
        redisClient.close();
    }
}
