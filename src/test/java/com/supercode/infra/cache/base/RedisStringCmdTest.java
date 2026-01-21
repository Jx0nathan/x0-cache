package com.supercode.infra.cache.base;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * 单机版的URL需要添加协议：前缀为redis://
 * 单机版 + SSL的URL需要添加协议：前缀为rediss://
 */
@Disabled
public class RedisStringCmdTest {

    private static SupercodeRedisClient redisClient = new SupercodeRedisClient("tf-usa-dev-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com", 6379);

    @Test
    public void testRedisTtl() {
        String redisKey = "testRedisTtl" + System.currentTimeMillis();
        redisClient.redisStringCmd().setex(redisKey, 1000, "11");
        long RedisKeyCmdTTL = redisClient.redisKeyCmd().ttl(redisKey);
        Assertions.assertTrue(RedisKeyCmdTTL >= 1000, "expireTtl:" + redisKey);
    }

    @Test
    public void testRedisString() {
        String standardStr = "just a str";
        redisClient.redisStringCmd().set("string_set", standardStr);
        String redisStr = redisClient.redisStringCmd().get("string_set");
        Assertions.assertEquals(standardStr, redisStr);

        redisClient.redisStringCmd().setnx("string_set_nx_1", "string_set_nx");
        String redisStrNx = redisClient.redisStringCmd().get("string_set_nx_1");
        Assertions.assertEquals("string_set_nx", redisStrNx);

        Long strLength = redisClient.redisStringCmd().strlen("string_set_nx_1");
        Assertions.assertEquals(13, strLength.longValue());

        redisClient.redisStringCmd().set("string_set_get_test", standardStr);
        String stringGetSet = redisClient.redisStringCmd().getset("string_set_get_test", "string_set_get");
        Assertions.assertEquals(standardStr, stringGetSet);
        String getSetStr = redisClient.redisStringCmd().get("string_set_nx");
        Assertions.assertEquals("string_set_get", getSetStr);

        redisClient.redisStringCmd().setex("string_set_ex", 2, "string_set_ex");
        String redisStrEx = redisClient.redisStringCmd().get("string_set_ex");
        Assertions.assertEquals("string_set_ex", redisStrEx);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException ex) {
            // ignore..
        }
        String redisStrExTwice = redisClient.redisStringCmd().get("string_set_ex");
        Assertions.assertNull(redisStrExTwice);
    }

    @Test
    public void testRedisStringAppend() {
        String standardStr = "just a str";
        redisClient.redisStringCmd().set("string_set", standardStr);
        redisClient.redisStringCmd().append("string_set", "test");
        Assertions.assertEquals(standardStr + "test", redisClient.redisStringCmd().get("string_set"));
    }

    @Test
    public void testRedisStringDescAndIncr() {
        Long descNum = redisClient.redisStringCmd().decr("string_desc");
        Assertions.assertEquals(-1, descNum.longValue());

        Long incrNum = redisClient.redisStringCmd().incr("string_incr");
        Assertions.assertEquals(1, incrNum.longValue());
    }

    @Test
    public void testRedisStringDescBy() {
        Long descNum = redisClient.redisStringCmd().decrby("string_descby_1", 10);
        Assertions.assertEquals(-10, descNum.longValue());

        Long incrNum = redisClient.redisStringCmd().incrby("string_incrby_1", 10);
        Assertions.assertEquals(10, incrNum.longValue());

        Double incrNumFloat = redisClient.redisStringCmd().incrbyfloat("string_incrby_1", 10.01);
        Assertions.assertEquals(20.01, incrNumFloat, 0.01d);
    }

    @Test
    public void testRedisStringRange() {
        redisClient.redisStringCmd().set("string_range", "jonathan is man");
        String expectStr = redisClient.redisStringCmd().getrange("string_range", 0, 1);
        Assertions.assertEquals(expectStr, "jo");
    }
}
