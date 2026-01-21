package com.supercode.infra.cache.redis.cmd;

import com.supercode.infra.cache.pojo.GeoObject;
import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoWithin;
import io.lettuce.core.api.sync.RedisGeoCommands;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class RedisGeoCmd<K, V> extends AbstractRedisCmd<K, V> {

    public RedisGeoCmd(SupercodeRedisClient<K, V> supercodeRedisClient) {
        super(supercodeRedisClient);
    }

    private <R> R doGeoCmd(Function<RedisGeoCommands<String, String>, R> geoCmd) {
        return super.doCmd(geoCmd);
    }

    public List<GeoWithin<String>> georadius(String key, double longitude, double latitude, double distance, GeoArgs geoArgs) {
        return this.doGeoCmd(cmd -> cmd.georadius(key, longitude, latitude, distance, GeoArgs.Unit.m, geoArgs));
    }

    public Set<String> georadius(String key, double longitude, double latitude, double distance) {
        return this.doGeoCmd(cmd -> cmd.georadius(key, longitude, latitude, distance, GeoArgs.Unit.m));
    }

    public Set<String> georadiusbymember(String key, String member, double distance) {
        return this.doGeoCmd(cmd -> cmd.georadiusbymember(key, member, distance, GeoArgs.Unit.m));
    }

    public Double geodist(String key, String from, String to) {
        return this.doGeoCmd(cmd -> cmd.geodist(key, from, to, GeoArgs.Unit.m));
    }

    public Long geoadd(String key, List<GeoObject> lngLatMember) {
        Object[] args = new Object[lngLatMember.size() * 3];
        for (int i = 0; i < lngLatMember.size(); i++) {
            GeoObject object = lngLatMember.get(i);
            args[3 * i] = object.getCoord().getLongitude();
            args[3 * i + 1] = object.getCoord().getLatitude();
            args[3 * i + 2] = object.getName();
        }
        return this.doGeoCmd(cmd -> cmd.geoadd(key, args));
    }

    /**
     * (1)m/km/ft/mi 指定的是计算范围时的单位
     * (2) WITHCOORD 将位置的经纬度一并返回
     * (3) WITHDIST 将位置与中心点之间的距离一并返回
     * (4) ASC 表示按距离从近到远排序，DESC 表示从远到近
     * (5) COUNT 限定返回的记录条数
     */
    public List<GeoWithin<String>> georadius(String redisKey, double longitude, double latitude, double distance, int limit) {
        GeoArgs geoArgs = new GeoArgs().withHash().withDistance().withCoordinates().withCount(limit).asc();
        return georadius(redisKey, longitude, latitude, distance, geoArgs);
    }
}
