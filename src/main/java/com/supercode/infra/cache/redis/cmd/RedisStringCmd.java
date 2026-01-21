package com.supercode.infra.cache.redis.cmd;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.KeyValue;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.SlotHash;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Redis关于字符串相关的操作
 *
 * @author jonathan.ji
 */
public class RedisStringCmd<K, V> extends AbstractRedisCmd<K, V> {

    public RedisStringCmd(SupercodeRedisClient<K, V> supercodeRedisClient) {
        super(supercodeRedisClient);
    }

    private <R> R doStringCmd(Function<RedisStringCommands<String, String>, R> stringCmd) {
        return super.doCmd(stringCmd);
    }

    /**
     * Append a value to a key
     * Time complexity: O(1)
     */
    public Long append(String key, String value) {
        return this.doStringCmd(cmd -> cmd.append(key, value));
    }

    /**
     * Decrement the integer value of a key by one
     * Time complexity: O(1)
     */
    public Long decr(String key) {
        return this.doStringCmd(cmd -> cmd.decr(key));
    }

    /**
     * Decrement the integer value of a key by the given number
     * Time complexity: O(1)
     */
    public Long decrby(String key, long amount) {
        return this.doStringCmd(cmd -> cmd.decrby(key, amount));
    }

    /**
     * Get the value of a key.
     * Time complexity: O(1)
     */
    public String get(String key) {
        return this.doStringCmd(cmd -> cmd.get(key));
    }

    /**
     * Returns the bit value at offset in the string value stored at key
     * Time complexity: O(1)
     */
    public Long getbit(String key, long offset) {
        return this.doStringCmd(cmd -> cmd.getbit(key, offset));
    }

    /**
     * Get a substring of the string stored at a key
     * Time complexity: O(N) where N is the length of the returned string
     */
    public String getrange(String key, long start, long end) {
        return this.doStringCmd(cmd -> cmd.getrange(key, start, end));
    }

    /**
     * Set the string value of a key
     * Time complexity: O(1)
     */
    public String set(String key, String value) {
        return this.doStringCmd(cmd -> cmd.set(key, value));
    }

    /**
     * Sets or clears the bit at offset in the string value stored at key
     * Time complexity: O(1)
     */
    public Long setbit(String key, long offset, int value) {
        return this.doStringCmd(cmd -> cmd.setbit(key, offset, value));
    }

    /**
     * Set the value and expiration of a key
     * Time complexity: O(1)
     */
    public String setex(String key, long seconds, String value) {
        return this.doStringCmd(cmd -> cmd.setex(key, seconds, value));
    }

    /**
     * Set the value and expiration in milliseconds of a key
     * Time complexity: O(1)
     */
    public String psetex(String key, long milliseconds, String value) {
        return this.doStringCmd(cmd -> cmd.psetex(key, milliseconds, value));
    }

    /**
     * Set the value of a key, only if the key does not exist
     * Time complexity: O(1)
     */
    public Boolean setnx(String key, String value) {
        return this.doStringCmd(cmd -> cmd.setnx(key, value));
    }

    public Boolean setnx(String key, String value, long seconds) {
        return this.doStringCmd(cmd -> cmd.setnx(key, value)) && this.supercodeRedisClient.redisKeyCmd().expire(key, seconds);
    }

    /**
     * Overwrite part of a string at key starting at the specified offset
     * Time complexity: O(1), not counting the time taken to copy the new string in place.
     * Usually, this string is very small so the amortized complexity is O(1). Otherwise, complexity is O(M) with M being the length of the value argument.
     */
    public Long setrange(String key, long offset, String value) {
        return this.doStringCmd(cmd -> cmd.setrange(key, offset, value));
    }

    /**
     * Get the length of the value stored in a key
     * Time complexity: O(1)
     */
    public Long strlen(String key) {
        return this.doStringCmd(cmd -> cmd.strlen(key));
    }

    /**
     * Set the string value of a key and return its old value
     * Time complexity: O(1)
     */
    public String getset(String key, String value) {
        return this.doStringCmd(cmd -> cmd.getset(key, value));
    }

    /**
     * Increment the integer value of a key by one
     * Time complexity: O(1)
     */
    public Long incr(String key) {
        return this.doStringCmd(cmd -> cmd.incr(key));
    }

    /**
     * Increment the integer value of a key by the given amount
     * Time complexity: O(1)
     */
    public Long incrby(String key, long amount) {
        return this.doStringCmd(cmd -> cmd.incrby(key, amount));
    }

    /**
     * Increment the float value of a key by the given amount
     * Time complexity: O(1)
     */
    public Double incrbyfloat(String key, double amount) {
        return this.doStringCmd(cmd -> cmd.incrbyfloat(key, amount));
    }

    /**
     * Get the values of all the given keys
     * Time complexity: O(N) where N is the number of keys to retrieve.
     */
    public List<KeyValue<String, String>> mget(String... keys) {
        return this.doStringCmd(cmd -> cmd.mget(keys));
    }

    /**
     * Set multiple keys to multiple values
     * Time complexity: O(N) where N is the number of keys to set.
     */
    public String mset(Map<String, String> kv) {
        return this.doStringCmd(cmd -> cmd.mset(kv));
    }

    /**
     * Set multiple keys to multiple values, only if none of the keys exist
     * Time complexity: O(N) where N is the number of keys to set.
     */
    public Boolean msetnx(Map<String, String> kv) {
        if (kv.size() == 0) {
            return true;
        }
        Integer keySlot = null;
        for (String key : kv.keySet()) {
            int slot = SlotHash.getSlot(key);
            if (keySlot == null) {
                keySlot = slot;
            } else if (slot != keySlot) {
                return false;
            }
        }
        return this.doStringCmd(cmd -> cmd.msetnx(kv));
    }

    public String getAndDel(String key) {
        String value = get(key);
        supercodeRedisClient.redisKeyCmd().del(key);
        return value;
    }

    public long getLong(String key) {
        return Long.parseLong(StringUtils.defaultString(get(key), "0"));
    }
}
