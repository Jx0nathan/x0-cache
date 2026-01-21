package com.supercode.infra.cache.redis.cmd;

import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.KeyValue;
import io.lettuce.core.api.sync.RedisListCommands;

import java.util.List;
import java.util.function.Function;


/**
 * 与Redis key相关的操作
 *
 * @author @author jonathan.ji
 */
public class RedisListCmd<K, V> extends AbstractRedisCmd<K, V> {

    public RedisListCmd(SupercodeRedisClient<K, V> supercodeRedisClient) {
        super(supercodeRedisClient);
    }

    private <R> R doListCmd(Function<RedisListCommands<String, String>, R> listCmd) {
        return super.doCmd(listCmd);
    }

    /**
     * Get an element from a list by its index
     * Time complexity: O(N) where N is the number of elements to traverse to get to the element at index.
     * 谨慎使用
     */
    public String lindex(String key, long index) {
        return this.doListCmd(cmd -> cmd.lindex(key, index));
    }

    /**
     * Insert an element before or after another element in a list
     * Time complexity: O(N) where N is the number of elements to traverse before seeing the value pivot.
     * This means that inserting somewhere on the left end on the list (head) can be considered O(1) and inserting somewhere on the right end (tail) is O(N)
     */
    public Long linsert(String key, boolean before, String pivot, String value) {
        return this.doListCmd(cmd -> cmd.linsert(key, before, pivot, value));
    }

    /**
     * Get the length of a list
     * Time complexity: O(1)
     */
    public Long llen(String key) {
        return this.doListCmd(cmd -> cmd.llen(key));
    }

    /**
     * Remove and get the first element in a list
     * Time complexity: O(N) where N is the number of elements returned
     */
    public String lpop(String key) {
        return this.doListCmd(cmd -> cmd.lpop(key));
    }

    public KeyValue<String, String> blpop(String key, long timeout) {
        return this.doListCmd(cmd -> cmd.blpop(timeout, key));
    }

    /**
     * Prepend one or multiple values to a list
     * Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments
     */
    public Long lpush(String key, String... values) {
        return this.doListCmd(cmd -> cmd.lpush(key, values));
    }

    /**
     * Prepend values to a list, only if the list exists（只有当key存在的时候，才会插入数据）
     * <p>
     * Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments
     * 当N的数量是成千上万的话，请谨慎使用
     */
    public Long lpushx(String key, String... values) {
        return this.doListCmd(cmd -> cmd.lpushx(key, values));
    }

    /**
     * Get a range of elements from a list
     * Time complexity: O(S+N) where S is the distance of start offset from HEAD for small lists, from nearest end (HEAD or TAIL) for large lists;
     * and N is the number of elements in the specified range
     * <p>
     * 谨慎使用
     */
    public List<String> lrange(String key, long start, long stop) {
        return this.doListCmd(cmd -> cmd.lrange(key, start, stop));
    }

    /**
     * Remove elements from a list
     *
     * @param count > 0: Remove elements equal to element moving from head to tail.
     *              count < 0: Remove elements equal to element moving from tail to head.
     *              count = 0: Remove all elements equal to element.
     *              <p>
     *              Time complexity: O(N+M) where N is the length of the list and M is the number of elements removed
     *              <p>
     *              LREM list -2 "hello" will remove the last two occurrences of "hello" in the list stored at list.
     */
    public Long lrem(String key, long count, String value) {
        return this.doListCmd(cmd -> cmd.lrem(key, count, value));
    }

    /**
     * Set the value of an element in a list by its index
     * Time complexity: O(N) where N is the length of the list. Setting either the first or the last element of the list is O(1).
     * 如果N是成千上万的话，谨慎使用
     */
    public String lset(String key, long index, String value) {
        return this.doListCmd((cmd) -> cmd.lset(key, index, value));
    }

    /**
     * Trim a list to the specified range
     * Time complexity: O(N) where N is the number of elements to be removed by the operation
     *
     * @return "OK"
     * <p>
     * 谨慎使用
     */
    public String ltrim(String key, long start, long stop) {
        return this.doListCmd(cmd -> cmd.ltrim(key, start, stop));
    }

    /**
     * Remove and get the last element in a list
     * <p>
     * Time complexity: O(N) where N is the number of provided keys
     */
    public String rpop(String key) {
        return this.doListCmd((cmd) -> cmd.rpop(key));
    }

    /**
     * Append one or multiple values to a list
     * Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments
     */
    public Long rpush(String key, String... values) {
        return this.doListCmd(cmd -> cmd.rpush(key, values));
    }

    /**
     * Inserts specified values at the tail of the list stored at key, only if key already exists and holds a list.
     * In contrary to RPUSH, no operation will be performed when key does not yet exist.
     * <p>
     * Time complexity: O(1) for each element added, so O(N) to add N elements when the command is called with multiple arguments.
     */
    public Long rpushx(String key, String... values) {
        return this.doListCmd((cmd) -> cmd.rpushx(key, values));
    }


}
