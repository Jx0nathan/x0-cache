package com.supercode.infra.cache.redis.cmd;

import com.supercode.infra.cache.exception.RedisInfraException;
import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.function.Function;

public abstract class AbstractRedisCmd<K, V> {
    SupercodeRedisClient<K, V> supercodeRedisClient;

    protected AbstractRedisCmd(SupercodeRedisClient<K, V> supercodeRedisClient) {
        this.supercodeRedisClient = supercodeRedisClient;
    }


    @SuppressWarnings("unchecked")
    protected <T, R> R doCmd(Function<T, R> cmd) {
        RedisCommands<String, String> txCmd = supercodeRedisClient.transactionCmd.get();
        if (txCmd != null) {
            return cmd.apply((T) txCmd);
        } else {
            try (StatefulConnection<String, String> connection = supercodeRedisClient.pool.borrowObject()) {
                Object redisCmd = supercodeRedisClient.isCluster ? ((StatefulRedisClusterConnection<String, String>) connection).sync()
                        : ((StatefulRedisConnection<String, String>) connection).sync();
                return (R) cmd.apply((T) redisCmd);
            } catch (Exception ex) {
                throw new RedisInfraException("failed to obtain redis connection", ex);
            }
        }
    }

    public GenericObjectPool<StatefulConnection<String, String>> getPool() {
        return supercodeRedisClient.getPool();
    }

    public boolean getClose() {
        return supercodeRedisClient.getCloseFlag();
    }

    public boolean getClusterFlag() {
        return supercodeRedisClient.getClusterFlag();
    }

    /**
     * Retrieve the cluster view and get a {@link RedisClusterNode} by its slot number
     */
    public String getNodeId(String key) {
        int keySlot = SlotHash.getSlot(key);
        return supercodeRedisClient.getClusterClient().getPartitions().getPartitionBySlot(keySlot).getNodeId();
    }
}
