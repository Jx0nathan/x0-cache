package com.supercode.infra.cache.base;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.ScoredValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

@Disabled
public class RedisSortedSetTest {

    private static SupercodeRedisClient redisClient = new SupercodeRedisClient("tf-usa-dev-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com", 6379);

    @Test
    public void redisSortedSetTest() {
        String redisKey = "sorted_test" + System.currentTimeMillis();
        ScoredValue<String> item1 = ScoredValue.just(10000d, "btc");
        ScoredValue<String> item2 = ScoredValue.just(5000d, "eth");
        ScoredValue<String> item3 = ScoredValue.just(500d, "bnb");
        redisClient.redisSortedSetCmd().zadd(redisKey, item1, item2, item3);

        // 按score排序列出，参数区间为排名范围，从小到大
        List<String> rangSortedSet = redisClient.redisSortedSetCmd().zrange(redisKey, 0, -1);
        for (int i = 0; i < rangSortedSet.size(); i++) {
            if (i == 0) {
                Assertions.assertEquals("bnb", rangSortedSet.get(i));
            }

            if (i == 1) {
                Assertions.assertEquals("eth", rangSortedSet.get(i));
            }

            if (i == 2) {
                Assertions.assertEquals("btc", rangSortedSet.get(i));
            }
        }

        // 按score逆序列出，参数区间为排名范围，从大到小
        List<String> zrevrangeSortedSet = redisClient.redisSortedSetCmd().zrevrange(redisKey, 0, -1);
        for (int i = 0; i < zrevrangeSortedSet.size(); i++) {
            if (i == 0) {
                Assertions.assertEquals("btc", zrevrangeSortedSet.get(i));
            }

            if (i == 1) {
                Assertions.assertEquals("eth", zrevrangeSortedSet.get(i));
            }

            if (i == 2) {
                Assertions.assertEquals("bnb", zrevrangeSortedSet.get(i));
            }
        }

        // 获取指定value的score
        double btcValue = redisClient.redisSortedSetCmd().zscore(redisKey, "btc");
        Assertions.assertEquals(10000, btcValue, 0.1);

        // 获取指定value的排名
        Long rankNum = redisClient.redisSortedSetCmd().zrank(redisKey, "btc");
        Assertions.assertEquals(2L, rankNum.longValue());

        redisClient.redisSortedSetCmd().zrem(redisKey, "bnb");
        List<String> newValue = redisClient.redisSortedSetCmd().zrange(redisKey, 0, -1);
        Assertions.assertEquals(Arrays.asList("eth", "btc"), newValue);
    }

    @Test
    public void redisZSetRangeByScoreTest() {
        String redisKey = "sorted_test" + System.currentTimeMillis();
        ScoredValue<String> item1 = ScoredValue.just(10000d, "btc");
        ScoredValue<String> item2 = ScoredValue.just(5000d, "eth");
        ScoredValue<String> item3 = ScoredValue.just(500d, "bnb");
        redisClient.redisSortedSetCmd().zadd(redisKey, item1, item2, item3);

        //默认闭区间
        List<String> rangSortedSet = redisClient.redisSortedSetCmd().zrangebyscore(redisKey, 500, 10000);
        for (int i = 0; i < rangSortedSet.size(); i++) {
            if (i == 0) {
                Assertions.assertEquals("bnb", rangSortedSet.get(i));
            }

            if (i == 1) {
                Assertions.assertEquals("eth", rangSortedSet.get(i));
            }

            if (i == 2) {
                Assertions.assertEquals("btc", rangSortedSet.get(i));
            }
        }


        //开区间测试（左开右开）
        List<String> zrevrangeSortedOpenSet = redisClient.redisSortedSetCmd().zrangebyscore(redisKey, 500, false, 10000, false);
        Assertions.assertEquals(1, zrevrangeSortedOpenSet.size());

        for (int i = 0; i < zrevrangeSortedOpenSet.size(); i++) {
            if (i == 0) {
                Assertions.assertEquals("eth", zrevrangeSortedOpenSet.get(i));
            }
        }

        //开区间测试（左开右闭）
        List<String> zrevrangeSortedOpenSet1 = redisClient.redisSortedSetCmd().zrangebyscore(redisKey, 500, false, 10000, true);
        Assertions.assertEquals(2, zrevrangeSortedOpenSet1.size());

        for (int i = 0; i < zrevrangeSortedOpenSet1.size(); i++) {
            if (i == 0) {
                Assertions.assertEquals("eth", zrevrangeSortedOpenSet1.get(i));
            }
            if (i == 1) {
                Assertions.assertEquals("btc", zrevrangeSortedOpenSet1.get(i));
            }
        }
        //开区间测试（左闭右开）
        List<String> zrevrangeSortedOpenSet2 = redisClient.redisSortedSetCmd().zrangebyscore(redisKey, 500, true, 10000, false);
        Assertions.assertEquals(2, zrevrangeSortedOpenSet2.size());

        for (int i = 0; i < zrevrangeSortedOpenSet2.size(); i++) {
            if (i == 0) {
                Assertions.assertEquals("bnb", zrevrangeSortedOpenSet2.get(i));
            }
            if (i == 1) {
                Assertions.assertEquals("eth", zrevrangeSortedOpenSet2.get(i));
            }
        }
        Long aLong = redisClient.redisSortedSetCmd().zcountByRange(redisKey, 501, 10000);
        Assertions.assertEquals(2L, aLong.longValue());
        Long aLong1 = redisClient.redisSortedSetCmd().zremrangeByScore(redisKey, 501, 10000);
        Assertions.assertEquals(2L, aLong1.longValue());
        Long aLong2 = redisClient.redisSortedSetCmd().zcountByRange(redisKey, 501, 10000);
        Assertions.assertEquals(0L, aLong2.longValue());

    }
}
