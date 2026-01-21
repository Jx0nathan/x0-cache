package com.supercode.infra.cache.base;

import com.supercode.infra.cache.pojo.GeoObject;
import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoWithin;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

@Disabled
public class RedisGeoHashCmdTest {

    private static SupercodeRedisClient redisClient = new SupercodeRedisClient("tf-usa-dev-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com", 6379);

    @Test
    public void testBaseHash() {
        String redisKey = "geo_hash_test" + System.currentTimeMillis();
        List<GeoObject> geoObjectList = new ArrayList<>();
        GeoObject geoObject1 = new GeoObject("Sicily", 121.5390027866, 31.2578476233);
        geoObjectList.add(geoObject1);
        redisClient.redisGeoCmd().geoadd(redisKey, geoObjectList);

        GeoArgs geoArgs = new GeoArgs().withHash().withDistance().withCoordinates().withCount(10).asc();
        List<GeoWithin<String>> geoWithinList = redisClient.redisGeoCmd().georadius(redisKey, 121.5390027866, 31.2578476233, 3000, geoArgs);
        for (GeoWithin<String> item : geoWithinList) {
            System.out.println(item);
        }
    }
}
