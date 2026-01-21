package com.supercode.infra.cache.base;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import com.supercode.infra.cache.redis.cmd.RedisBloomFilterCmd;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


/**
 * 单机版的URL需要添加协议：前缀为redis://
 * 单机版 + SSL的URL需要添加协议：前缀为rediss://
 */
@Disabled
public class RedisBloomFilterCmdTest {

    private static SupercodeRedisClient redisClient = new SupercodeRedisClient("tf-usa-dev-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com", 6379);

    @Test
    public void testBF() throws InterruptedException {
        String fliterName = "bf:filtername" + System.currentTimeMillis();
        RedisBloomFilterCmd redisBloomFilterCmd = redisClient.redisBloomFilterCmd(fliterName, 100);
        redisBloomFilterCmd.add("wangyu");
        assertEquals(true, redisBloomFilterCmd.exisits("wangyu"));
        assertEquals(false, redisBloomFilterCmd.exisits("wangyu1"));
        redisBloomFilterCmd.add("wangyu1x");
        redisBloomFilterCmd.add("wangyu2x");
        redisBloomFilterCmd.add("wangyu3x");
        redisBloomFilterCmd.add("wangyu4x");
        redisBloomFilterCmd.add("wangyu5x");
        assertEquals(true, redisBloomFilterCmd.exisits("wangyu2x"));
        assertEquals(false, redisBloomFilterCmd.exisits("tom"));
        assertEquals(false, redisBloomFilterCmd.exisits("cat"));
        assertEquals(false, redisBloomFilterCmd.exisits("spring"));
        assertEquals(false, redisBloomFilterCmd.exisits("mvc"));
        assertEquals(false, redisBloomFilterCmd.exisits("boot"));
        assertEquals(false, redisBloomFilterCmd.exisits("cloud"));
        assertEquals(false, redisBloomFilterCmd.exisits("eureka"));
        assertEquals(false, redisBloomFilterCmd.exisits("apollo"));
        assertEquals(false, redisBloomFilterCmd.exisits("xxljob"));
        assertEquals(true, redisBloomFilterCmd.exisits("wangyu1x"));
        assertEquals(true, redisBloomFilterCmd.exisits("wangyu3x"));
        assertEquals(true, redisBloomFilterCmd.exisits("wangyu4x"));
        assertEquals(true, redisBloomFilterCmd.exisits("wangyu5x"));
        fliterName = "bf:filtername" + ":10s";
        RedisBloomFilterCmd redisBloomFilterCmdWithEx = redisClient.redisBloomFilterCmd(fliterName, 100, 0.01, 10000);
        redisBloomFilterCmdWithEx.add("brave");
        assertEquals(true, redisBloomFilterCmdWithEx.exisits("brave"));
        Thread.sleep(2000);
        assertEquals(true, redisBloomFilterCmdWithEx.exisits("brave"));
        Thread.sleep(30000);
        assertEquals(false, redisBloomFilterCmdWithEx.exisits("brave"));

    }
}
