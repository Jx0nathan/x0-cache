package com.supercode.infra.cache.redis.cmd;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.KeyValue;
import io.lettuce.core.api.sync.RedisHashCommands;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 与Redis Hash相关的操作
 *
 * @author @author jonathan.ji
 */
public class RedisHashCmd<K, V> extends AbstractRedisCmd<K, V> {

    public RedisHashCmd(SupercodeRedisClient<K, V> supercodeRedisClient) {
        super(supercodeRedisClient);
    }

    private <R> R doHashCmd(Function<RedisHashCommands<String, String>, R> hashCmd) {
        return super.doCmd(hashCmd);
    }

    /**
     * Delete one or more hash fields
     * Time complexity: O(N) where N is the number of fields to be removed
     */
    public Long hdel(String key, String... fields) {
        return this.doHashCmd(cmd -> cmd.hdel(key, fields));
    }

    /**
     * Determine if a hash field exists
     * Time complexity: O(1)
     */
    public Boolean hexists(String key, String field) {
        return this.doHashCmd(cmd -> cmd.hexists(key, field));
    }

    /**
     * Get the value of a hash field
     * Time complexity: O(1)
     */
    public String hget(String key, String field) {
        return this.doHashCmd(cmd -> cmd.hget(key, field));
    }

    /**
     * Increment the integer value of a hash field by the given number
     * Time complexity: O(1)
     */
    public Long hincrby(String key, String field, long amount) {
        return this.doHashCmd(cmd -> cmd.hincrby(key, field, amount));
    }

    /**
     * Increment the float value of a hash field by the given amount
     * Time complexity: O(1)
     */
    public Double hincrbyfloat(String key, String field, double amount) {
        return this.doHashCmd(cmd -> cmd.hincrbyfloat(key, field, amount));
    }

    /**
     * Get all the fields and values in a hash
     * Time complexity: O(N) where N is the size of the hash
     */
    public Map<String, String> hgetall(String key) {
        return this.doHashCmd(cmd -> cmd.hgetall(key));
    }

    /**
     * Get all the fields in a hash
     * Time complexity: O(N) where N is the size of the hash
     */
    public List<String> hkeys(String key) {
        return this.doHashCmd((cmd) -> cmd.hkeys(key));
    }

    /**
     * Get the number of fields in a hash
     * Time complexity: O(1)
     */
    public Long hlen(String key) {
        return this.doHashCmd(cmd -> cmd.hlen(key));
    }

    /**
     * Get the values of all the given hash fields
     * Time complexity: O(N) where N is the number of fields being requested
     */
    public List<KeyValue<String, String>> hmget(String key, String... fields) {
        return this.doHashCmd(cmd -> cmd.hmget(key, fields));
    }

    /**
     * Set multiple hash fields to multiple value
     * Time complexity: O(N) where N is the number of fields being set
     */
    public String hmset(String key, Map<String, String> map) {
        return this.doHashCmd(cmd -> cmd.hmset(key, map));
    }

    /**
     * Set the string value of a hash field
     * Time complexity: O(1) for each field/value pair added, so O(N) to add N field/value pairs when the command is called with multiple field/value pairs
     */
    public Boolean hset(String key, String field, String value) {
        return this.doHashCmd(cmd -> cmd.hset(key, field, value));
    }

    /**
     * Set the value of a hash field, only if the field does not exist
     * Time complexity: O(1)
     */
    public Boolean hsetnx(String key, String field, String value) {
        return this.doHashCmd(cmd -> cmd.hsetnx(key, field, value));
    }

    /**
     * Get the string length of the field value in a hash
     * Time complexity: O(1)
     */
    public Long hstrlen(String key, String field) {
        return this.doHashCmd((cmd) -> cmd.hstrlen(key, field));
    }

    /**
     * Get all the values in a hash
     * Time complexity: O(N) where N is the size of the hash.
     */
    public List<String> hvals(String key) {
        return this.doHashCmd((cmd) -> cmd.hvals(key));
    }

}
