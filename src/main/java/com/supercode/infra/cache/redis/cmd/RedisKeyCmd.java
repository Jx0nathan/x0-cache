package com.supercode.infra.cache.redis.cmd;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.api.sync.RedisKeyCommands;

import java.util.Date;
import java.util.function.Function;

/**
 * 与Redis key相关的操作
 *
 * @author @author jonathan.ji
 */
public class RedisKeyCmd<K, V> extends AbstractRedisCmd<K, V> {

    public RedisKeyCmd(SupercodeRedisClient<K, V> supercodeRedisClient) {
        super(supercodeRedisClient);
    }

    private <R> R doKeyCmd(Function<RedisKeyCommands<String, String>, R> keyCmd) {
        return super.doCmd(keyCmd);
    }

    /**
     * Determine how many keys exist
     * Time complexity: O(N) where N is the number of keys to check
     */
    public Long exists(String... keys) {
        return this.doKeyCmd(cmd -> cmd.exists(keys));
    }

    /**
     * Set a key's time to live in seconds
     * Time complexity: O(1)
     */
    public Boolean expire(String key, long seconds) {
        return this.doKeyCmd(cmd -> cmd.expire(key, seconds));
    }

    /**
     * Delete one or more keys
     * Time complexity: O(N) where N is the number of keys that will be removed.
     * When a key to remove holds a value other than a string, the individual complexity for this key is O(M) where M is the number of elements in the list, set, sorted set or hash.
     * Removing a single key that holds a string value is O(1).
     */
    public Long del(String... keys) {
        return this.doKeyCmd(cmd -> cmd.del(keys));
    }

    /**
     * Set the expiration for a key as a UNIX timestamp
     * Time complexity: O(1)
     */
    public Boolean expireat(String key, Date timestamp) {
        return this.doKeyCmd(cmd -> cmd.expireat(key, timestamp));
    }

    /**
     * Set a key's time to live in milliseconds
     * Time complexity: O(1)
     */
    public Boolean pexpire(String key, long milliseconds) {
        return this.doKeyCmd(cmd -> cmd.pexpire(key, milliseconds));
    }

    /**
     * Set the expiration for a key as a UNIX timestamp specified in milliseconds
     * Time complexity: O(1)
     */
    public Boolean pexpireat(String key, long timestamp) {
        return this.doKeyCmd(cmd -> cmd.pexpireat(key, timestamp));
    }

    /**
     * Get the time to live for a key in milliseconds
     * Time complexity: O(1)
     */
    public Long pttl(String key) {
        return this.doKeyCmd(cmd -> cmd.pttl(key));
    }

    /**
     * Get the time to live for a key
     * <p>
     * The command returns -2 if the key does not exist.
     * The command returns -1 if the key exists but has no associated expire
     * <p>
     * TTL in seconds, or a negative value in order to signal an error (see the description above).
     * <p>
     * Time complexity: O(1)
     */
    public Long ttl(String key) {
        return this.doKeyCmd(cmd -> cmd.ttl(key));
    }

    /**
     * Determine the type stored at key
     * Time complexity: O(1)
     */
    public String type(String key) {
        return this.doKeyCmd(cmd -> cmd.type(key));
    }
}
