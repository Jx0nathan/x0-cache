package com.supercode.infra.cache.redis.cmd;

import com.supercode.infra.cache.exception.RedisInfraException;
import com.supercode.infra.cache.exception.RedisTransactionException;
import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;

import java.util.function.Function;

/**
 * MULTI, EXEC, DISCARD and WATCH are the foundation of transactions in Redis
 * <p>
 * > MULTI
 * OK
 * > INCR foo
 * QUEUED
 * > INCR bar
 * QUEUED
 * > EXEC
 * 1) (integer) 1
 * 2) (integer) 1
 * <p>
 * 所有的命令都会在EXEC执行之后执行，前期的命令都存在队列中
 * <p>
 * 需要注意：
 * （1）Redis的事务不满足原子性
 * <p>
 * {@link "https://redis.io/topics/transactions"}
 *
 * @author jonathan.ji
 */
public class RedisTransactionCmd<K, V> extends AbstractRedisCmd<K, V> {

    private ThreadLocal<StatefulConnection<String, String>> transactionConn;

    private ThreadLocal<RedisCommands<String, String>> transactionCmd;

    public RedisTransactionCmd(SupercodeRedisClient<K, V> supercodeRedisClient,
                               ThreadLocal<StatefulConnection<String, String>> transactionConn,
                               ThreadLocal<RedisCommands<String, String>> transactionCmd) {
        super(supercodeRedisClient);
        this.transactionConn = transactionConn;
        this.transactionCmd = transactionCmd;
    }

    public void beginTransaction(String key) {
        if (this.transactionConn.get() != null) {
            throw new RedisTransactionException("another transaction is in progress");
        }
        try {
            StatefulConnection<String, String> conn;
            RedisCommands<String, String> cmd;
            if (super.getClusterFlag()) {
                StatefulRedisClusterConnection<String, String> clusterConn = (StatefulRedisClusterConnection<String,
                        String>) super.getPool().borrowObject();
                conn = clusterConn;
                cmd = clusterConn.getConnection(super.getNodeId(key)).sync();
                cmd.unwatch();
            } else {
                StatefulRedisConnection<String, String> redisConn = (StatefulRedisConnection<String, String>)
                        super.getPool().borrowObject();
                conn = redisConn;
                cmd = redisConn.sync();
                cmd.unwatch();
            }
            this.transactionConn.set(conn);
            this.transactionCmd.set(cmd);
        } catch (Exception e) {
            this.transactionConn.remove();
            this.transactionCmd.remove();
            throw new RedisTransactionException("failed to begin transaction", e);
        }
    }

    public void watch(String... keys) {
        RedisCommands<String, String> cmd = this.transactionCmd.get();
        StatefulConnection<String, String> conn = this.transactionConn.get();
        if (cmd == null || conn == null) {
            throw new RedisTransactionException("watch not in a transaction");
        }
        cmd.watch(keys);
    }

    public void multi() {
        RedisCommands<String, String> cmd = this.transactionCmd.get();
        StatefulConnection<String, String> conn = this.transactionConn.get();
        if (cmd == null || conn == null) {
            throw new RedisTransactionException("multi not in a transaction");
        }
        cmd.multi();
    }

    public TransactionResult commit() {
        RedisCommands<String, String> cmd = this.transactionCmd.get();
        StatefulConnection<String, String> conn = this.transactionConn.get();
        if (cmd == null || conn == null) {
            throw new RedisTransactionException("commit not in a transaction");
        }
        try {
            TransactionResult execResult = cmd.exec();
            if (execResult.size() == 0) {
                throw new RedisTransactionException("watched key changed");
            }
            return execResult;
        } catch (Exception e) {
            throw new RedisTransactionException("failed to commit transaction", e);
        } finally {
            super.getPool().returnObject(conn);
            this.transactionCmd.remove();
            this.transactionConn.remove();
        }
    }

    public void rollback() {
        RedisCommands<String, String> cmd = this.transactionCmd.get();
        StatefulConnection<String, String> conn = this.transactionConn.get();
        if (cmd == null || conn == null) {
            throw new RedisTransactionException("rollback not in a transaction");
        }
        try {
            cmd.discard();
        } catch (Exception e) {
            throw new RedisTransactionException("failed to rollback transaction", e);
        } finally {
            super.getPool().returnObject(conn);
            this.transactionCmd.remove();
            this.transactionConn.remove();
        }
    }

    public boolean cas(String key, String expected, String newVal) {
        return this.doSingleNodeCmd(key, cmd -> {
            cmd.watch(key);
            String currentVal = cmd.get(key);
            if (expected == null) {
                if (currentVal != null) {
                    return false;
                }
            } else if (!expected.equals(currentVal)) {
                cmd.unwatch();
                return false;
            }
            cmd.multi();
            cmd.set(key, newVal);
            TransactionResult result = cmd.exec();
            return result != null;
        });
    }

    private <R> R doSingleNodeCmd(String key, Function<RedisCommands<String, String>, R> cmd) {
        try (StatefulConnection<String, String> conn = super.getPool().borrowObject()) {
            StatefulRedisConnection<String, String> nodeConn;
            if (super.getClusterFlag()) {
                nodeConn = ((StatefulRedisClusterConnection<String, String>) conn).getConnection(this.getNodeId(key));
            } else {
                nodeConn = (StatefulRedisConnection<String, String>) conn;
            }
            return cmd.apply(nodeConn.sync());
        } catch (Exception e) {
            throw new RedisInfraException("failed to obtain redis connection", e);
        }
    }
}
