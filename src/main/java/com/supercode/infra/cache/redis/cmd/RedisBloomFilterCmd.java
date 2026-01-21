package com.supercode.infra.cache.redis.cmd;

import com.google.common.base.Preconditions;
import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import com.supercode.master.utils.text.HashUtil;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.function.Function;

/**
 * @author :brave
 * @date : 2023/4/17
 */
@Log4j2
public class RedisBloomFilterCmd<K, V> extends AbstractRedisCmd<K, V> {
    private volatile long size;
    private volatile int hashIterations;
    private RedisCommands<String, String> sync;
    private final String filterName;
    private final long expiration;
    /**
     * 默认误判率
     */
    private final double errorRate;
    private final long expectedInsertions;

    /**
     * @param filterName         过滤器名称
     * @param expectedInsertions 预期插入的元素数量
     * @param errorRate          误差率（误差率越小性能相对越差，误差率的设置可根据自己业务情况设定，默认0.03）
     * @param expiration         过期时间
     */
    public RedisBloomFilterCmd(long expectedInsertions, double errorRate,
                               String filterName, long expiration, SupercodeRedisClient<K, V> supercodeRedisClient) {
        super(supercodeRedisClient);
        this.filterName = filterName;
        this.expiration = expiration;
        this.errorRate = errorRate;
        this.expectedInsertions = expectedInsertions;
        init();
    }

    private <R> R doBFCmd(Function<RedisStringCommands<String, String>, R> stringCmd) {
        return super.doCmd(stringCmd);
    }


    /**
     * 计算布隆过滤器所需要的位数大小，
     *
     * @param n
     * @param p
     * @return
     */
    private long optimalNumOfBits(long n, double p) {
        if (p == 0) {
            p = Double.MIN_VALUE;
        }
        return (long) (-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    /**
     * 计算需要的hash函数数量
     *
     * @param expectedInsertions 预期插入的元素数量
     * @param bitSize            位数大小
     * @return
     */
    private int optimalNumOfHashFunctions(long expectedInsertions, long bitSize) {
        return Math.max(1, (int) Math.round((double) bitSize / expectedInsertions * Math.log(2)));
    }

    /**
     * bitset的长度不能大于2的32次方
     *
     * @return
     */
    protected long getMaxSize() {
        return Integer.MAX_VALUE * 2L;
    }


    /**
     * 初始化，如果存在则不初始化
     */
    private void init() {
        Preconditions.checkArgument(!(errorRate > 1), "Bloom filter false probability can't be greater than 1");
        Preconditions.checkArgument(!(errorRate < 0), "Bloom filter false probability can't be negative");
        size = optimalNumOfBits(expectedInsertions, errorRate);
        Preconditions.checkArgument(!(size == 0), "Bloom filter calculated size is " + size);
        Preconditions.checkArgument(!(size > getMaxSize()), "Bloom filter size can't be greater than " + getMaxSize() + ". But calculated size is " + size);
        hashIterations = optimalNumOfHashFunctions(expectedInsertions, size);
        Preconditions.checkArgument(!StringUtils.isEmpty(filterName), "Bloom filter name can't be empty");
        if (getSync().exists(filterName) > 0) {
            log.info("Bloom filter {} already exists, skip init", filterName);
            return;
        }
        this.doBFCmd(cmd -> getSync().setbit(filterName, size - 1, 0));
        if (expiration > 0L) {
            this.doBFCmd(cmd -> getSync().pexpire(filterName, expiration));
        }
    }

    /**
     * 计算下标
     */
    private long[] hash(String value) {
        long[] offset = new long[hashIterations];
        for (int i = 1; i <= hashIterations; i++) {
            offset[i - 1] = Math.abs(HashUtil.murmur128AsLong(value + i) % size);
        }
        return offset;
    }

    private RedisCommands<String, String> getSync() {
        if (sync == null) {
            try {
                StatefulConnection<String, String> connection = super.getPool().borrowObject();
                StatefulRedisConnection<String, String> nodeConn;
                if (super.getClusterFlag()) {
                    nodeConn = ((StatefulRedisClusterConnection<String, String>) connection).getConnection(this.getNodeId(filterName));
                } else {
                    nodeConn = (StatefulRedisConnection<String, String>) connection;
                }
                sync = nodeConn.sync();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return sync;
    }

    /**
     * 添加元素
     *
     * @param element
     * @return
     */
    public boolean add(String element) {
        for (long index : hash(element)) {
            this.doBFCmd(cmd -> getSync().setbit(filterName, index, 1));
        }
        return true;
    }

    /**
     * 判断元素是否存在
     *
     * @param element
     * @return
     */
    public boolean exisits(String element) {
        for (long index : hash(element)) {
            if (this.doBFCmd(cmd -> getSync().getbit(filterName, index)) == 0L) {
                return false;
            }
        }
        return true;
    }
}
