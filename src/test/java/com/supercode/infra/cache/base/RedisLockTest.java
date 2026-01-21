package com.supercode.infra.cache.base;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisLockTest {

    private final Logger logger = LoggerFactory.getLogger(SupercodeRedisClient.class);

    SupercodeRedisClient<String, String> redisClient;

    @BeforeEach
    public void before() {
        redisClient = new SupercodeRedisClient<>("dcs-sit-common-redis-cluster.dma0mq.clustercfg.apse1.cache.amazonaws.com", 6379);
    }

    @Test
    public void testIsLockedOtherThread() throws InterruptedException {
        String lockName = "testIsLockedOtherThread" + System.currentTimeMillis();
        redisClient.redisLockCmd().tryRedLock(lockName, 3000);

        Thread t = new Thread(() -> {
            boolean lockResult = redisClient.redisLockCmd().tryRedLock(lockName, 3000);
            Assertions.assertFalse(lockResult);
        });

        t.start();
        t.join();
        redisClient.redisLockCmd().releaseRedLock(lockName);

        Thread t2 = new Thread(() -> {
            boolean lockResult = redisClient.redisLockCmd().tryRedLock(lockName, 3000);
            Assertions.assertTrue(lockResult);
        });
        t2.start();
        t2.join();
    }

    @Test
    public void testIsLock() {
        String lockName = "testIsLock" + System.currentTimeMillis();

        for (int i = 0; i < 10; i++) {
            boolean lockResult = redisClient.redisLockCmd().tryRedLock(lockName, 300000);
            Assertions.assertTrue(lockResult);
        }

        boolean leaseLock = redisClient.redisLockCmd().releaseRedLock(lockName);
        Assertions.assertTrue(leaseLock);
    }

    @Test
    public void testLockAndExecuteTask() throws InterruptedException {
        String lockName = "testIsLock" + System.currentTimeMillis();
        boolean lockResult1 = redisClient.redisLockCmd().tryRedLockAndExecuteTask(lockName, 50000, false, () -> System.out.println("xxx"));
        Assertions.assertTrue(lockResult1);
        Thread thread = new Thread(() -> {
            boolean lockResult2 = redisClient.redisLockCmd().tryRedLockAndExecuteTask(lockName, 50000, true, () -> System.out.println("xxxx"));
            Assertions.assertTrue(lockResult2);//锁自动释放
        });
        thread.start();
        thread.join();
        boolean lockResult3 = redisClient.redisLockCmd().tryRedLockAndExecuteTask(lockName, 50000, true, () -> {
            System.out.println(2 / 0);
        });
        Assertions.assertFalse(lockResult3);//任务执行失败，返回false，释放锁
        boolean lockResult4 = redisClient.redisLockCmd().tryRedLockAndExecuteTask(lockName, 50000, false, () -> System.out.println("xxx"));
        Assertions.assertTrue(lockResult4);//任务执行失败，也自动释放锁
    }

    @Test
    public void testLockAndExecuteTaskWithReturn() throws InterruptedException {
        String lockName = "testIsLock" + System.currentTimeMillis();
        String lockResult1 = redisClient.redisLockCmd().tryRedLockAndExecuteTask(lockName, 50000, false, () -> {
            System.out.println("xxx");
            return "sss";
        });
        Assertions.assertEquals(lockResult1, "sss");
        Thread thread = new Thread(() -> {
            boolean lockResult2 = redisClient.redisLockCmd().tryRedLockAndExecuteTask(lockName, 50000, true, () -> {
                System.out.println("xxxx");
                return true;
            });
            Assertions.assertTrue(lockResult2);//锁自动释放
        });
        thread.start();
        thread.join();
        int lockResult3 = redisClient.redisLockCmd().tryRedLockAndExecuteTask(lockName, 50000, true, () -> {
            System.out.println("2 / 0");
            return 1;
        });
        Assertions.assertEquals(lockResult3, 1);

        boolean lockResult4 = redisClient.redisLockCmd().tryRedLockAndExecuteTask(lockName, 50000, false, () -> {
            System.out.println("xxx");
            return false;
        });
        Assertions.assertFalse(lockResult4);//任务执行失败，也自动释放锁
    }

    @Test
    public void testLockRetry() {
        String lockName = "testLockRetry";
        boolean lockResultOne = redisClient.redisLockCmd().tryRedLockWithRetry(lockName);//锁住了，5秒
        Assertions.assertTrue(lockResultOne);

        boolean lockResultTwo = redisClient.redisLockCmd().tryRedLockWithRetry(lockName);//会重试，默认重试1分钟，最终会锁住
        Assertions.assertTrue(lockResultTwo);


        boolean lockResultThree = redisClient.redisLockCmd().tryRedLockWithRetry(lockName, 8000, 1000);//现在还是锁的，锁的时间为5秒，重试1秒，一定锁不住
        Assertions.assertFalse(lockResultThree);

        boolean lockResultFour = redisClient.redisLockCmd().tryRedLockWithRetry(lockName, 5000);//会锁住，5秒
        Assertions.assertTrue(lockResultFour);


        boolean lockResultFive = redisClient.redisLockCmd().tryRedLockWithRetry(lockName, 2000);//不会锁住
        Assertions.assertFalse(lockResultFive);


        boolean lockResultSix = redisClient.redisLockCmd().tryRedLockWithRetry(lockName, 20000, 6000);//会锁住锁20秒
        Assertions.assertTrue(lockResultSix);


        boolean lockResultseven = redisClient.redisLockCmd().tryRedLockWithRetry(lockName, 10000, 25000);//会锁住，锁10秒
        Assertions.assertTrue(lockResultseven);

        boolean lockResultEight = redisClient.redisLockCmd().tryRedLockWithRetry(lockName, 10000, 2000);//不会锁住，还差8秒
        Assertions.assertFalse(lockResultEight);

        boolean lockResultNight = redisClient.redisLockCmd().tryRedLockWithRetry(lockName, 10000, 5000, 12000);//会锁住，锁10秒
        Assertions.assertTrue(lockResultNight);

        boolean lockResultTen = redisClient.redisLockCmd().tryRedLockWithRetry(lockName, 10000, 5000, 15000);//会锁住，继续锁10秒
        Assertions.assertTrue(lockResultTen);

        boolean lockResultEleven = redisClient.redisLockCmd().tryRedLockWithRetry(lockName, 10000, 5000, 3000);//不会锁住，
        Assertions.assertFalse(lockResultEleven);
    }


    /**
     * 大致看一下每个锁对应的key的过期时间
     */
    @Test
    public void testLockTTl() {
        for (int i = 0; i < 100; i++) {
            redisClient.redisLockCmd().tryRedLock("test" + i, 3000);
            String lockName = redisClient.redisLockCmd().getLockKey("test" + i);
            long ttl = redisClient.redisKeyCmd().ttl(lockName);
            System.out.println("every time " + i + " ttl is " + ttl);
        }
    }

    /**
     * 测试锁的重入性
     */
    @Test
    public void testLockInOneThread() {
        String lockName = "testLockInOneThread" + System.currentTimeMillis();
        String redisLockName = redisClient.redisLockCmd().getLockKey(lockName);
        try {
            boolean lockResult = redisClient.redisLockCmd().tryRedLock(lockName, 3000);
            Assertions.assertTrue(lockResult);
            Long firstExpireTtl = redisClient.redisKeyCmd().ttl(redisLockName);
            System.out.println("firstLock " + lockName + " - " + Thread.currentThread().getId() + " lockResult: " + lockResult + " firstExpireTtl: " + firstExpireTtl);
            Thread.sleep(500);

            boolean secondLockResult = redisClient.redisLockCmd().tryRedLock(lockName, 3000);
            Assertions.assertTrue(secondLockResult);

            Long expireTtl = redisClient.redisKeyCmd().ttl(redisLockName);
            System.out.println("secondLock " + redisLockName + " - " + Thread.currentThread().getId() + " lockResult: " + lockResult + " expireTtl : " + expireTtl);
            Assertions.assertTrue(expireTtl <= 3000, "expireTtl=" + expireTtl);
            Assertions.assertTrue(expireTtl >= 2000, "expireTtl=" + expireTtl);
        } catch (Exception ex) {
            logger.error("ex : " + ex);
        }
    }

    /**
     * 一个线程获取锁成功，一个线程获取锁失败
     */
    @Test
    public void testLockOneByOne() {
        Assertions.assertTimeout(Duration.ofSeconds(10), () -> {

            final CountDownLatch startSignal = new CountDownLatch(1);
            final CountDownLatch testSignal = new CountDownLatch(1);
            final CountDownLatch completeSignal = new CountDownLatch(2);

            final String lockName = "lock1";
            new Thread(() -> {
                try {
                    startSignal.await();
                    boolean lockResult = redisClient.redisLockCmd().tryRedLock(lockName);
                    Assertions.assertTrue(lockResult);
                    System.out.println("lock " + lockName + " - " + Thread.currentThread().getId() + " lockResult: " + lockResult);
                    testSignal.countDown();
                    Thread.sleep(500);
                    boolean releaseLock = redisClient.redisLockCmd().releaseRedLock(lockName);
                    Assertions.assertTrue(releaseLock);
                    System.out.println("lock " + lockName + " - " + Thread.currentThread().getId() + " releaseLock: " + releaseLock);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                completeSignal.countDown();
            }).start();

            new Thread(() -> {
                try {
                    testSignal.await();
                    boolean lockResult = redisClient.redisLockCmd().tryRedLock(lockName);
                    Assertions.assertFalse(lockResult);
                    System.out.println("lock " + lockName + " - " + Thread.currentThread().getId() + " lockResult: " + lockResult);
                    boolean releaseLock = redisClient.redisLockCmd().releaseRedLock(lockName);
                    Assertions.assertFalse(releaseLock);
                    System.out.println("lock " + lockName + " - " + Thread.currentThread().getId() + " releaseLock: " + releaseLock);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
                completeSignal.countDown();
            }).start();

            System.out.println("start");
            startSignal.countDown();
            completeSignal.await();
            System.out.println("complete");
        });
    }

    /**
     * 500个请求顺序请求加锁的行为
     */
    @Test
    public void testMultipleLocksOneByOne() throws InterruptedException {
        ExecutorService executorService = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
                1000L, TimeUnit.SECONDS,
                new SynchronousQueue<>());

        String lockName = "testMultipleLocksOneByOne" + System.currentTimeMillis();
        AtomicInteger acquireLocks = new AtomicInteger();
        for (int i = 0; i < 500; i++) {
            executorService.submit(() -> {
                try {
                    redisClient.redisLockCmd().tryRedLock(lockName, 1);
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    acquireLocks.incrementAndGet();
                } finally {
                    redisClient.redisLockCmd().releaseRedLock(lockName);
                }
            });
        }
        executorService.shutdown();
        assertThat(executorService.awaitTermination(3, TimeUnit.MINUTES)).isTrue();
        assertThat(acquireLocks.get()).isEqualTo(500);
    }

    /**
     * 模仿在加锁的时间范围内，有多个线程同时在请求
     */
    @Test
    public void testMultipleLocksInOneTimeRange() throws InterruptedException {
        String lockName = "testMultipleLocksInOneTimeRange" + System.currentTimeMillis();

        int threadNum = 100;
        CountDownLatch mainCountDownLatch = new CountDownLatch(threadNum);

        CountDownLatch firstRunCountDownLatch = new CountDownLatch(1);
        // 模拟一个业务在5秒内部获取锁
        new Thread(() -> {
            try {
                boolean lockResult = redisClient.redisLockCmd().tryRedLock(lockName, 5000);
                Assertions.assertTrue(lockResult);
                System.out.println("getLock " + lockName + " - " + Thread.currentThread().getId());
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                firstRunCountDownLatch.countDown();
            }
        }).start();

        // 在一定时间之间的加锁请求都是失败的，同时他们释放锁的请求也都是失败的
        AtomicInteger getLockFailNum = new AtomicInteger();
        AtomicInteger releaseLockFailNum = new AtomicInteger();
        for (int i = 0; i < threadNum; i++) {
            firstRunCountDownLatch.await();
            Executors.newCachedThreadPool().submit(() -> {
                try {
                    boolean lockResult = redisClient.redisLockCmd().tryRedLock(lockName, 5000);
                    if (!lockResult) {
                        getLockFailNum.incrementAndGet();
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    mainCountDownLatch.countDown();
                } catch (Exception ex) {
                    ex.printStackTrace();
                } finally {
                    boolean lockResult = redisClient.redisLockCmd().releaseRedLock(lockName);
                    if (!lockResult) {
                        releaseLockFailNum.incrementAndGet();
                    }
                }
            });
        }
        mainCountDownLatch.await();
        Assertions.assertEquals(100, getLockFailNum.get());
        Assertions.assertEquals(100, releaseLockFailNum.get());
        redisClient.redisLockCmd().releaseRedLock(lockName);
    }

    @Test
    public void testLockWhenMasterIsDown() throws InterruptedException {
        String lockName = "testLockWhenMasterIsDown" + System.currentTimeMillis();
        CountDownLatch mainThreadCountDownLatch = new CountDownLatch(1);
        // 模拟一个业务获取一把锁，锁定时间是5分钟
        new Thread(() -> {
            try {
                boolean lockResult = redisClient.redisLockCmd().tryRedLock(lockName, 300000);
                Assertions.assertTrue(lockResult);
                System.out.println("getLock " + lockName + " - " + Thread.currentThread().getId());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            mainThreadCountDownLatch.countDown();
        }).start();

        // 在此期间将当前的主节点挂掉，实现主从切换，期间是另外一个线程理论上也无法获取到锁
        mainThreadCountDownLatch.await();
        boolean getLockFlag = false;
        long startTime = System.currentTimeMillis();
        long endTime = startTime + 200000;
        while (endTime > startTime) {
            boolean lockResult = redisClient.redisLockCmd().tryRedLock(lockName);
            if (lockResult) {
                getLockFlag = true;
            }
            startTime = System.currentTimeMillis();
        }
        Assertions.assertFalse(getLockFlag);
        redisClient.redisLockCmd().tryRedLock(lockName);
    }
}
