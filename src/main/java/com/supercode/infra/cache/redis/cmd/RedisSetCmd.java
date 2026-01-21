package com.supercode.infra.cache.redis.cmd;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.api.sync.RedisSetCommands;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class RedisSetCmd<K, V> extends AbstractRedisCmd<K, V> {

    public RedisSetCmd(SupercodeRedisClient<K, V> supercodeRedisClient) {
        super(supercodeRedisClient);
    }

    private <R> R doSetCmd(Function<RedisSetCommands<String, String>, R> setCmd) {
        return super.doCmd(setCmd);
    }

    /**
     * Add one or more members to a set.
     * <p>
     * Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
     */
    public Long sadd(String key, String... members) {
        return this.doSetCmd(cmd -> cmd.sadd(key, members));
    }

    /**
     * Get the number of members in a set
     * Time complexity: O(1)
     */
    public Long scard(String key) {
        return this.doSetCmd(cmd -> cmd.scard(key));
    }

    /**
     * Determine if a given value is a member of a set
     * <p>
     * Time complexity: O(1)
     */
    public Boolean sismember(String key, String member) {
        return this.doSetCmd(cmd -> cmd.sismember(key, member));
    }

    /**
     * Move a member from one set to another
     * <p>
     * Time complexity: O(1)
     */
    public Boolean smove(String source, String destination, String member) {
        return this.doSetCmd(cmd -> cmd.smove(source, destination, member));
    }

    /**
     * Get all the members in a set.
     * Time complexity: O(N) where N is the set cardinality
     */
    public Set<String> smembers(String key) {
        return this.doSetCmd(cmd -> cmd.smembers(key));
    }

    /**
     * Remove and return a random member from a set.
     * Time complexity: Without the count argument O(1), otherwise O(N) where N is the value of the passed count.
     */
    public String spop(String key) {
        return this.doSetCmd(cmd -> cmd.spop(key));
    }

    /**
     * Remove and return one or multiple random members from a set
     * Time complexity: Without the count argument O(1), otherwise O(N) where N is the value of the passed count.
     */
    public Set<String> spop(String key, long count) {
        return this.doSetCmd(cmd -> cmd.spop(key, count));
    }

    /**
     * Get one random member from a set
     * Time complexity: Without the count argument O(1), otherwise O(N) where N is the absolute value of the passed count.
     */
    public String srandmember(String key) {
        return this.doSetCmd(cmd -> cmd.srandmember(key));
    }

    /**
     * Get one or multiple random members from a set
     * Time complexity: Without the count argument O(1), otherwise O(N) where N is the absolute value of the passed count
     */
    public List<String> srandmember(String key, long count) {
        return this.doSetCmd(cmd -> cmd.srandmember(key, count));
    }

    /**
     * Remove one or more members from a set
     * Time complexity: O(N) where N is the number of members to be removed
     */
    public Long srem(String key, String... members) {
        return this.doSetCmd(cmd -> cmd.srem(key, members));
    }

}
