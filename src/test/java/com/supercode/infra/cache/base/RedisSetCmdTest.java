package com.supercode.infra.cache.base;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Set;

@Disabled
public class RedisSetCmdTest {

    private static SupercodeRedisClient redisClient = new SupercodeRedisClient("tf-usa-dev-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com", 6379);

    @Test
    public void redisSetTest() {
        String redisKey = "coins_set" + System.currentTimeMillis();
        redisClient.redisSetCmd().sadd(redisKey, "btc", "eth", "bnb");
        redisClient.redisSetCmd().sadd(redisKey, "btc");

        Set<String> redisSet = redisClient.redisSetCmd().smembers(redisKey);
        Assertions.assertEquals(3, redisSet.size());

        Boolean isExit = redisClient.redisSetCmd().sismember(redisKey, "btc");
        Assertions.assertTrue(isExit);

        Long redisSetNum = redisClient.redisSetCmd().scard(redisKey);
        Assertions.assertEquals(3L, redisSetNum.longValue());
    }
}
