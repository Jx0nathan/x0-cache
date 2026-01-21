package com.supercode.infra.cache.base;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class RedisHllCmdTest {

    private static SupercodeRedisClient redisClient = new SupercodeRedisClient("tf-usa-dev-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com", 6379);

    @Test
    public void testRedisHllHash() {
        String redisKey = "hll" + System.currentTimeMillis();
        redisClient.redisHllCmd().pfadd(redisKey, "a", "b", "c", "d", "e", "f", "g");
        long count = redisClient.redisHllCmd().pfcount(redisKey);
        Assertions.assertEquals(7, count);
    }
}
