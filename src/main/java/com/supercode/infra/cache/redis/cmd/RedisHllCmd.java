package com.supercode.infra.cache.redis.cmd;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.api.sync.RedisHLLCommands;

import java.util.function.Function;

public class RedisHllCmd<K, V> extends AbstractRedisCmd<K, V> {

    public RedisHllCmd(SupercodeRedisClient<K, V> supercodeRedisClient) {
        super(supercodeRedisClient);
    }

    private <R> R doHllCmd(Function<RedisHLLCommands<String, String>, R> hllCmd) {
        return super.doCmd(hllCmd);
    }

    /**
     * Adds all the element arguments to the HyperLogLog data structure
     * Time complexity: O(1) to add every element.
     */
    public void pfadd(String key, String... values) {
        this.doHllCmd(cmd -> cmd.pfadd(key, values));
    }

    /**
     * Time complexity: O(1) with a very small average constant time when called with a single key.
     * O(N) with N being the number of keys, and much bigger constant times, when called with multiple keys.
     */
    public Long pfcount(String key) {
        return this.doHllCmd(cmd -> cmd.pfcount(key));
    }
}
