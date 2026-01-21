package com.supercode.infra.cache.base;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RedisInstanceTest {

    SupercodeRedisClient<String, String> redisClient;

    @BeforeEach
    public void before() {
        redisClient = new SupercodeRedisClient<>("tf-usa-dev-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com", 6389);
    }

    @Test
    public void testRedisFailed() {
        String lockName = "testRedisFailed" + System.currentTimeMillis();
        try {
            redisClient.redisLockCmd().tryRedLock(lockName);
        } catch (Exception ex) {
            System.out.println(ex);
        }

    }
}
