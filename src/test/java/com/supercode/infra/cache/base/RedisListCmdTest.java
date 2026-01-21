package com.supercode.infra.cache.base;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.KeyValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Disabled
public class RedisListCmdTest {

    private static SupercodeRedisClient redisClient = new SupercodeRedisClient("tf-usa-dev-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com", 6379);

    /**
     * 右边进左边出
     */
    @Test
    public void testRpush() {
        String redisKey = "coins" + System.currentTimeMillis();

        redisClient.redisListCmd().rpush(redisKey, "btc", "bnb", "etc");

        Long len = redisClient.redisListCmd().llen(redisKey);
        Assertions.assertEquals(3, len.longValue());

        String coin1 = redisClient.redisListCmd().lpop(redisKey);
        Assertions.assertEquals("btc", coin1);

        String coin2 = redisClient.redisListCmd().lpop(redisKey);
        Assertions.assertEquals("bnb", coin2);

        String coin3 = redisClient.redisListCmd().lpop(redisKey);
        Assertions.assertEquals("etc", coin3);

        String coin4 = redisClient.redisListCmd().lpop(redisKey);
        Assertions.assertNull(coin4);
    }

    @Test
    public void testBlpop() {
        String redisKey = "coins" + System.currentTimeMillis();

        redisClient.redisListCmd().rpush(redisKey, "btc", "bnb", "etc");
        KeyValue blpop = redisClient.redisListCmd().blpop(redisKey, 3);
        System.out.println(blpop.getValue() + " " + blpop.getKey());
    }

    @Test
    public void testRpush2() {
        String redisKey = "infra" + System.currentTimeMillis();
        redisClient.redisListCmd().rpush(redisKey, "redis", "mq", "eureka");

        String value = redisClient.redisListCmd().lindex(redisKey, 1);
        Assertions.assertEquals("mq", value);

        List<String> rangeValueList = redisClient.redisListCmd().lrange(redisKey, 0, 1);
        Assertions.assertEquals(Arrays.asList("redis", "mq"), rangeValueList);
    }

    /**
     * -1 is the last element of the list
     */
    @Test
    public void testLtrim() {
        String redisKey = "ltrim" + System.currentTimeMillis();
        redisClient.redisListCmd().rpush(redisKey, "one", "two", "three");
        redisClient.redisListCmd().ltrim(redisKey, 1, -1);
        List<String> trimList = redisClient.redisListCmd().lrange(redisKey, 0, -1);
        Assertions.assertEquals(Arrays.asList("two", "three"), trimList);
    }

    @Test
    public void testLinsert() {
        String redisKey = "linsert" + System.currentTimeMillis();
        redisClient.redisListCmd().rpush(redisKey, "Hello");
        redisClient.redisListCmd().rpush(redisKey, "World");
        redisClient.redisListCmd().linsert(redisKey, true, "World", "There");
        List<String> linsertList = redisClient.redisListCmd().lrange(redisKey, 0, -1);
        Assertions.assertEquals(Arrays.asList("Hello", "There", "World"), linsertList);
    }

    @Test
    public void testLpush() {
        String redisKey = "lpush" + System.currentTimeMillis();
        redisClient.redisListCmd().lpush(redisKey, "world");
        redisClient.redisListCmd().lpush(redisKey, "hello");
        Assertions.assertEquals(Arrays.asList("hello", "world"), redisClient.redisListCmd().lrange(redisKey, 0, -1));
    }

    @Test
    public void testLpushx() {
        String redisKey1 = "lpushx" + System.currentTimeMillis();
        redisClient.redisListCmd().lpush(redisKey1, "world");
        redisClient.redisListCmd().lpushx(redisKey1, "hello");
        Assertions.assertEquals(Arrays.asList("hello", "world"), redisClient.redisListCmd().lrange(redisKey1, 0, -1));

        redisClient.redisListCmd().lpushx(redisKey1 + "tmp", "hello");
        Assertions.assertEquals(Collections.emptyList(), redisClient.redisListCmd().lrange(redisKey1 + "tmp", 0, -1));
    }

    @Test
    public void testLrem() {
        String redisKey = "ltem" + System.currentTimeMillis();
        redisClient.redisListCmd().rpush(redisKey, "hello");
        redisClient.redisListCmd().rpush(redisKey, "hello");
        redisClient.redisListCmd().rpush(redisKey, "foo");
        redisClient.redisListCmd().rpush(redisKey, "hello");

        redisClient.redisListCmd().lrem(redisKey, -2, "hello");

        Assertions.assertEquals(Arrays.asList("hello", "foo"), redisClient.redisListCmd().lrange(redisKey, 0, -1));
    }

    @Test
    public void testLset() {
        String redisKey = "lset" + System.currentTimeMillis();
        redisClient.redisListCmd().rpush(redisKey, "one");
        redisClient.redisListCmd().rpush(redisKey, "two");
        redisClient.redisListCmd().rpush(redisKey, "three");

        redisClient.redisListCmd().lset(redisKey, 0, "four");
        redisClient.redisListCmd().lset(redisKey, -2, "five");

        Assertions.assertEquals(Arrays.asList("four", "five", "three"), redisClient.redisListCmd().lrange(redisKey, 0, -1));
    }

    @Test
    public void testRpushx() {
        String redisKey1 = "rpushx" + System.currentTimeMillis();
        redisClient.redisListCmd().rpush(redisKey1, "Hello");
        redisClient.redisListCmd().rpushx(redisKey1, "World");

        String redisKey2 = "rpushx" + System.currentTimeMillis();
        redisClient.redisListCmd().rpushx(redisKey2, "World");

        Assertions.assertEquals(Arrays.asList("Hello", "World"), redisClient.redisListCmd().lrange(redisKey1, 0, -1));
        Assertions.assertEquals(Collections.emptyList(), redisClient.redisListCmd().lrange(redisKey2, 0, -1));
    }
}
