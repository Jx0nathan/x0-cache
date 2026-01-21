package com.supercode.infra.cache.base;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

@Disabled
public class RedisHashCmdTest {

    private static SupercodeRedisClient redisClient = new SupercodeRedisClient("tf-usa-dev-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com", 6379);

    @Test
    public void testBaseHash() {
        String redisKey = "redisHash" + System.currentTimeMillis();
        redisClient.redisHashCmd().hset(redisKey, "field", "Hello");
        String value = redisClient.redisHashCmd().hget(redisKey, "field");
        Assertions.assertEquals("Hello", value);

        long fieldNum = redisClient.redisHashCmd().hlen(redisKey);
        Assertions.assertEquals(1, fieldNum);

        boolean isExit = redisClient.redisHashCmd().hexists(redisKey, "field");
        Assertions.assertTrue(isExit);

        Long delFlag = redisClient.redisHashCmd().hdel(redisKey, "field");
        Assertions.assertEquals(1L, delFlag.longValue());

        boolean isMissed = redisClient.redisHashCmd().hexists(redisKey, "field");
        Assertions.assertFalse(isMissed);
    }

    @Test
    public void test() {
        String redisKey = "redisHash" + System.currentTimeMillis();

        redisClient.redisHashCmd().hset(redisKey, "field1", "field1");
        redisClient.redisHashCmd().hset(redisKey, "field2", "field2");
        redisClient.redisHashCmd().hset(redisKey, "field3", "field3");
        redisClient.redisHashCmd().hset(redisKey, "field4", "field4");
        redisClient.redisHashCmd().hset(redisKey, "field5", "field5");

        Map<String, String> fieldHash = redisClient.redisHashCmd().hgetall(redisKey);
        Assertions.assertEquals(5, fieldHash.size());

        List<String> hashList = redisClient.redisHashCmd().hkeys(redisKey);
        List<String> valueList = redisClient.redisHashCmd().hvals(redisKey);
        long fieldLength = redisClient.redisHashCmd().hstrlen(redisKey, "field1");
        Assertions.assertEquals(6, fieldLength);
    }
}
