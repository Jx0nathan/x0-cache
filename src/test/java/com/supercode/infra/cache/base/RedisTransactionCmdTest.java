package com.supercode.infra.cache.base;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class RedisTransactionCmdTest {

    private static SupercodeRedisClient redisClient = new SupercodeRedisClient("tf-usa-dev-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com", 6379);

    /**
     * 事务得是在一个同一个分片上，可与使用tag来保证，否则报错：
     * CROSSSLOT Keys in request don't hash to the same slot
     */
    @Test
    public void testTransaction() {
        String redisKey = "transactions" + System.currentTimeMillis();

        // 开始事务
        redisClient.transactionCmd().beginTransaction(redisKey);
        redisClient.transactionCmd().multi();
        redisClient.redisStringCmd().incr(redisKey);
        redisClient.redisStringCmd().incr(redisKey);

        // 在事务没有完成之前，获取不到数据
        Assertions.assertNull(redisClient.redisStringCmd().get(redisKey));

        // 事务提交之后的数据
        redisClient.transactionCmd().commit();
        Assertions.assertEquals("2", redisClient.redisStringCmd().get(redisKey));
    }

    /**
     * 如果语法错误，客户端不执行队列中的命令
     */
    @Test
    public void testTransactionAtomicity01() {
        String redisKey1 = "books" + System.currentTimeMillis();
        String redisKey2 = "author" + System.currentTimeMillis();

        // 开始事务
        redisClient.transactionCmd().beginTransaction(redisKey1);
        redisClient.transactionCmd().multi();

        // 执行具体的事务
        redisClient.redisStringCmd().set(redisKey1, "redis{111}");
        redisClient.redisStringCmd().incr(redisKey1); // 是一个错误语法的命令
        redisClient.redisStringCmd().set(redisKey2, "jonathan.ji{111}");

        // 提交事务
        redisClient.transactionCmd().commit();

        Assertions.assertEquals("redis", redisClient.redisStringCmd().get(redisKey1));
        Assertions.assertEquals("jonathan.ji", redisClient.redisStringCmd().get(redisKey2));
    }

    /**
     * 如果执行错误，不保证原子性
     * <p>
     * ERROR : WRONGTYPE Operation against a key holding the wrong kind of value
     */
    @Test
    public void testTransactionAtomicity02() {
        String redisKey = "booksAtomicity02" + System.currentTimeMillis();

        // 开始事务
        redisClient.transactionCmd().beginTransaction(redisKey);
        redisClient.transactionCmd().multi();

        // 执行具体的事务
        redisClient.redisStringCmd().set(redisKey, "1");
        redisClient.redisSetCmd().sadd(redisKey, "2", "3"); // 错误的命令
        redisClient.redisStringCmd().set(redisKey, "4");

        // 提交事务
        redisClient.transactionCmd().commit();
        Assertions.assertEquals("4", redisClient.redisStringCmd().get(redisKey));
    }
}
