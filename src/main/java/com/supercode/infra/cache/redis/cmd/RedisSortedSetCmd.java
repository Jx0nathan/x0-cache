package com.supercode.infra.cache.redis.cmd;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.sync.RedisSortedSetCommands;

import java.util.List;
import java.util.function.Function;

/**
 * @author jonathan.ji
 */
public class RedisSortedSetCmd<K, V> extends AbstractRedisCmd<K, V> {

    public RedisSortedSetCmd(SupercodeRedisClient<K, V> supercodeRedisClient) {
        super(supercodeRedisClient);
    }

    private <R> R doSortSetCmd(Function<RedisSortedSetCommands<String, String>, R> sortedSetCmd) {
        return super.doCmd(sortedSetCmd);
    }

    /**
     * Add one or more members to a sorted set, or update its score if it already exists.
     * <p>
     * Time complexity: O(log(N)) for each item added, where N is the number of elements in the sorted set
     *
     * @return Long integer-reply specifically:
     * <p>
     * The number of elements added to the sorted sets, not including elements already existing for which the score was
     * updated.
     */
    public Long zadd(String key, ScoredValue<V>... scoredValue) {
        return this.doSortSetCmd(cmd -> cmd.zadd(key, scoredValue));
    }

    /**
     * Add one or more members to a sorted set, or update its score if it already exists.
     */
    public Long zadd(String key, double score, String member) {
        return this.doSortSetCmd(cmd -> cmd.zadd(key, score, member));
    }

    /**
     * Return a range of members in a sorted set, by index
     * <p>
     * Time complexity: O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned.
     */
    public List<String> zrange(String key, long start, long stop) {
        return this.doSortSetCmd(cmd -> cmd.zrange(key, start, stop));
    }


    /**
     * Return a range of members in a sorted set, by score.
     * The default is closed interval, that is, greater than or equal to and less than or equal to. Please call if you have other requirements
     * zrangebyscore(String key, double min, boolean isMinInclude, double max, boolean isMaxInclude)
     */
    public List<String> zrangebyscore(String key, double min, double max) {
        return this.zrangebyscore(key, min, true, max, true);
    }

    /**
     * Return a range of members in a sorted set, by score.
     *
     * @param isMinInclude is min Closed interval?
     * @param isMaxInclude is max Closed interval?
     */
    public List<String> zrangebyscore(String key, double min, boolean isMinInclude, double max, boolean isMaxInclude) {
        Range range = Range.from(isMinInclude ? Range.Boundary.including(min) : Range.Boundary.excluding(min),
                isMaxInclude ? Range.Boundary.including(max) : Range.Boundary.excluding(max));
        return this.doSortSetCmd(cmd -> cmd.zrangebyscore(key, range));
    }

    /**
     * Return a range of members in a sorted set, by index, with scores ordered from high to low
     * <p>
     * Time complexity: O(log(N)+M) with N being the number of elements in the sorted set and M the number of elements returned
     */
    public List<String> zrevrange(String key, long start, long stop) {
        return this.doSortSetCmd(cmd -> cmd.zrevrange(key, start, stop));
    }

    /**
     * Remove one or more members from a sorted set
     * <p>
     * Time complexity: O(M*log(N)) with N being the number of elements in the sorted set and M the number of elements to be removed
     */
    public Long zrem(String key, String... members) {
        return this.doSortSetCmd((cmd) -> cmd.zrem(key, members));
    }

    /**
     * Get the score associated with the given member in a sorted set.
     * Time complexity: O(1)
     */
    public Double zscore(String key, String value) {
        return this.doSortSetCmd(cmd -> cmd.zscore(key, value));
    }

    /**
     * Determine the index of a member in a sorted set
     * Time complexity: O(log(N))
     */
    public Long zrank(String key, String value) {
        return this.doSortSetCmd(cmd -> cmd.zrank(key, value));
    }

    /**
     * Remove all members in a sorted set within the given indexes.
     */
    public Long zremrangebyrank(String key, long start, long stop) {
        return this.doSortSetCmd(cmd -> cmd.zremrangebyrank(key, start, stop));
    }

    /**
     * Get the number of members in a sorted set.
     */
    public Long zcard(String key) {
        return this.doSortSetCmd(cmd -> cmd.zcard(key));
    }

    /**
     * Count the members in a sorted set with scores within the given Range.
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long zcountByRange(String key, double min, double max) {
        return this.doSortSetCmd(cmd -> cmd.zcount(key, Range.create(min, max)));
    }

    /**
     * Remove all members in a sorted set within the given scores.     *
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Long zremrangeByScore(String key, double min, double max) {
        return this.doSortSetCmd(cmd -> cmd.zremrangebyscore(key, Range.create(min, max)));
    }
}
