package com.supercode.infra.cache.lock;

import com.supercode.infra.cache.exception.RedisLockException;
import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import com.supercode.infra.cache.redis.cmd.AbstractRedisCmd;
import com.supercode.master.monitor.annotation.Metrics;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Supplier;

public class RedisLock<K, V> extends AbstractRedisCmd<K, V> {

    private final Logger logger = LoggerFactory.getLogger(RedisLock.class);

    private static final String RED_LOCK_HASH_TAG = "redlock";

    /**
     * 不需要显示的remove
     */
    private final ThreadLocal<Map<String, String>> redLockThreadLocal = new ThreadLocal<>();

    private static final int MIN_RED_LOCK_REPLICA_COUNT = 2;

    private static final String RETRY_TIME_IS_TOO_LONG = "retry time is too long";

    private static final long RED_LOCK_DEFAULT_MAX_RETRY_TIME = 5 * 60 * 1000L;

    private static final long RED_LOCK_DEFAULT_DEFAULT_RETRY_TIME = 60 * 1000L;

    private static final String RED_LOCK_SCRIPT = """
            if redis.call("get",KEYS[1]) == ARGV[1] then
            	return redis.call("del",KEYS[1])
            else
            	return 0
            end""";

    private static final String RED_LOCK_REFRESH_SCRIPT = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("pexpire", KEYS[1], ARGV[2])
            else
                return 0
            end""";

    private static final String EXISTS_SCRIPT = "return redis.call(\"exists\",KEYS[1])";

    public RedisLock(SupercodeRedisClient<K, V> supercodeRedisClient) {
        super(supercodeRedisClient);
    }

    /**
     * 默认重试时长为1分钟
     *
     * @param lock 业务传入的key名
     * @return
     */
    @Metrics
    public boolean tryRedLockWithRetry(String lock) {
        boolean locked = this.tryRedLock(lock);
        if (locked) {
            return locked;
        }
        return commontryRedLockWithRetry(() -> this.tryRedLock(lock), RED_LOCK_DEFAULT_DEFAULT_RETRY_TIME);
    }

    private boolean commontryRedLockWithRetry(Callable<Boolean> command, long retryTime) {
        boolean locked = false;
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < retryTime) {
            try {
                locked = command.call();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (locked) {
                return locked;
            }
            try {
                Thread.sleep(5l);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return locked;
    }


    /**
     * @param lock      业务传入的key名
     * @param retryTime 当获取锁失败时重新获取锁的重试时间（无限循环至这个时间点，最长不能超过5分钟）
     * @return
     */
    @Metrics
    public boolean tryRedLockWithRetry(String lock, long retryTime) {
        if (retryTime > RED_LOCK_DEFAULT_MAX_RETRY_TIME) {
            throw new RedisLockException(RETRY_TIME_IS_TOO_LONG);
        }
        boolean locked = this.tryRedLock(lock);
        if (locked) {
            return locked;
        }
        return commontryRedLockWithRetry(() -> this.tryRedLock(lock), retryTime);
    }

    /**
     * @param lock                业务传入的key名
     * @param maxLockMilliseconds 锁过期时间
     * @param retryTimeoutMillis  当获取锁失败时重新获取锁的重试时间（无限循环至这个时间点，最长不能超过5分钟）
     * @return
     */
    @Metrics
    public boolean tryRedLockWithRetry(String lock, long maxLockMilliseconds, long retryTimeoutMillis) {
        if (retryTimeoutMillis > RED_LOCK_DEFAULT_MAX_RETRY_TIME) {
            throw new RedisLockException(RETRY_TIME_IS_TOO_LONG);
        }
        boolean locked = this.tryRedLock(lock);
        if (locked) {
            return locked;
        }
        return commontryRedLockWithRetry(() -> this.tryRedLock(lock, maxLockMilliseconds), retryTimeoutMillis);

    }

    /**
     * @param lock                业务传入的key名
     * @param maxLockMilliseconds 锁过期时间
     * @param maxWaitMilliseconds 等待主从数据同步的时间，最大为锁过期时间
     * @param retryTimeoutMillis  当获取锁失败时重新获取锁的重试时间（无限循环至这个时间点，最长不能超过5分钟）
     * @return
     */
    @Metrics
    public boolean tryRedLockWithRetry(String lock, long maxLockMilliseconds, long maxWaitMilliseconds, long retryTimeoutMillis) {
        if (retryTimeoutMillis > RED_LOCK_DEFAULT_MAX_RETRY_TIME) {
            throw new RedisLockException(RETRY_TIME_IS_TOO_LONG);
        }
        boolean locked = this.tryRedLock(lock);
        if (locked) {
            return locked;
        }
        return commontryRedLockWithRetry(() -> this.tryRedLock(lock, maxLockMilliseconds, maxWaitMilliseconds), retryTimeoutMillis);
    }

    public boolean tryRedLock(String lock) {
        return this.tryRedLock(lock, 5000);
    }

    /**
     * 上锁+执行task+主动释放锁
     *
     * @param lock            业务传入的key名
     * @param maxLockMilliseconds 过期时间
     * @param task            执行的任务(无入参无返回值)，拿到锁之后执行
     * @param isThrow         加锁失败是否抛出异常
     * @return 返回获取锁成功并且任务执行成功并且释放锁成功
     */
    public boolean tryRedLockAndExecuteTask(String lock, long maxLockMilliseconds, boolean isThrow, Runnable task) {
        if (task == null) {
            throw new IllegalArgumentException("task cannot be null!");
        }
        boolean islock = this.tryRedLock(lock, maxLockMilliseconds);
        if (!islock) {
            if (isThrow) {
                throw new RedisLockException("Redis lock is already locked!");
            } else {
                logger.warn("Redis lock is already locked!");
                return false;
            }
        }
        try {
            task.run();
        } catch (Throwable e) {
            logger.error("the task run failed!", e);
            throw e;
        } finally {
            this.releaseRedLock(lock);
        }
        return true;
    }

    /**
     * 上锁+执行task+主动释放锁
     *
     * @param lock            业务传入的key名
     * @param maxLockMilliseconds 过期时间
     * @param task            执行的任务(无入参有返回值)，拿到锁之后执行
     * @return 返回获取锁成功并且任务执行成功并且释放锁成功
     */
    public <R> R tryRedLockAndExecuteTask(String lock, long maxLockMilliseconds, boolean isThrow, Supplier<R> task) {
        if (task == null) {
            throw new IllegalArgumentException("task cannot be null!");
        }
        boolean islock = this.tryRedLock(lock, maxLockMilliseconds);
        if (!islock) {
            if (isThrow) {
                throw new RedisLockException("Redis lock is already locked!");
            } else {
                logger.warn("Redis lock is already locked!");
                return null;
            }
        }
        R r = null;
        try {
            r = task.get();
        } catch (Throwable e) {
            logger.error("the task run failed!", e);
            throw e;
        } finally {
            this.releaseRedLock(lock);
        }
        return r;
    }

    public boolean tryRedLock(String lock, long maxLockMilliseconds) {
        return this.tryRedLock(lock, maxLockMilliseconds, 500);
    }

    /**
     * @param lock                lock name
     * @param maxLockMilliseconds lock time in milliseconds
     * @param maxWaitMilliseconds 等待主从数据同步的时间，最大为锁过期时间
     */
    public boolean tryRedLock(String lock, long maxLockMilliseconds, long maxWaitMilliseconds) {
        boolean closing = super.getClose();
        if (closing) {
            return false;
        }
        if (maxLockMilliseconds == 0) {
            return false;
        }
        if (maxWaitMilliseconds > maxLockMilliseconds) {
            maxWaitMilliseconds = maxLockMilliseconds;
        }

        // Build redis key
        String resource = this.getLockKey(lock);

        // re-entrance, refresh expiration
        if (this.redLockThreadLocal.get() != null) {
            Map<String, String> redLocks = this.redLockThreadLocal.get();
            if (redLocks.containsKey(lock)) {
                long result = this.doSingleNodeCmd(resource, (cmd) ->
                        cmd.eval(RED_LOCK_REFRESH_SCRIPT, ScriptOutputType.INTEGER,
                                new String[]{resource}, redLocks.get(lock), String.valueOf(maxLockMilliseconds))
                );
                return result == 1;
            }
        }

        // generate unique id
        String sign = UUID.randomUUID().toString();
        logger.info("RedisLock.getLock sign is {} resource is {} ", sign, resource);
        if (super.getClusterFlag()) {
            // determine whether it is a cluster
            return this.doClusterLock(lock, resource, sign, maxWaitMilliseconds, maxLockMilliseconds);
        } else {
            // determine whether it is a master-slave
            return this.doMasterSlaveLock(lock, resource, sign, maxLockMilliseconds);
        }
    }

    /**
     * release lock
     */
    public boolean releaseRedLock(String lock) {
        Map<String, String> redLocks = this.redLockThreadLocal.get();
        if (redLocks == null) {
            return false;
        }
        if (!redLocks.containsKey(lock)) {
            return false;
        }
        String resource = this.getLockKey(lock);
        String sign = redLocks.get(lock);
        boolean success = this.doSingleNodeCmd(resource, cmd -> {
            long result = cmd.eval(RED_LOCK_SCRIPT, ScriptOutputType.INTEGER, new String[]{resource}, sign);
            return result > 0;
        });
        if (success) {
            redLocks.remove(lock);
        } else {
            boolean exists = this.doSingleNodeCmd(resource, cmd -> cmd.eval(EXISTS_SCRIPT, ScriptOutputType.BOOLEAN, new String[]{resource}));
            if (!exists) {
                redLocks.remove(lock);
                return true;
            }
        }
        return success;
    }

    /**
     * determine whether it is a cluster
     */
    private boolean doClusterLock(String lock, String resource, String sign, long maxWaitMilliseconds, long maxLockMilliseconds) {
        boolean masterSuccess = false;
        boolean rollback = false;

        long getPoolStartTime = System.currentTimeMillis();
        try (StatefulConnection<String, String> conn = super.getPool().borrowObject()) {
            long getPoolEndTime = System.currentTimeMillis();
            logger.debug("the lock {} get connection time is : {}", lock, (getPoolEndTime - getPoolStartTime));
            StatefulRedisClusterConnection<String, String> clusterConn = (StatefulRedisClusterConnection<String, String>) conn;
            long beginTime = System.currentTimeMillis();


            // Get the connection via the current node
            String masterId = super.getNodeId(resource);
            RedisCommands<String, String> masterCmd = clusterConn.getConnection(masterId).sync();
            String masterResult = masterCmd.set(resource, sign, SetArgs.Builder.nx().px(maxLockMilliseconds));
            masterSuccess = "OK".equals(masterResult);
            if (!masterSuccess) {
                return false;
            }


            // Wait for slave sync if master nodes is 1 ; slave nodes is 2 ; slaveNodes is 2
            Map<RedisClusterNode, RedisAsyncCommands<String, String>> slaveNodes =
                    clusterConn.async()
                            .slaves(node -> masterId.equals(node.getSlaveOf()))
                            .asMap();
            int slaveCount = slaveNodes.size();

            if (slaveCount > 0) {
                int minReplicaCount = Math.min(slaveCount, MIN_RED_LOCK_REPLICA_COUNT);
                masterCmd.waitForReplication(minReplicaCount, maxWaitMilliseconds);
            }

            // check time elapsed
            long endTime = System.currentTimeMillis();
            if (endTime - beginTime >= maxLockMilliseconds) {
                // print the time-consuming of each step
                throw new RedisLockException("slave sync too slow");
            }

            Map<String, String> redlocks = this.redLockThreadLocal.get();
            if (redlocks == null) {
                redlocks = new HashMap<>();
                this.redLockThreadLocal.set(redlocks);
            }

            redlocks.put(lock, sign);
            return true;
        } catch (Exception ex) {
            rollback = true;
            logger.info("failed to acquire redLock: " + ex.getMessage(), ex);
            return false;
        } finally {
            if (masterSuccess && rollback) {
                // unfortunately, i want to delete the data
                this.doSingleNodeCmd(resource, cmd ->
                        cmd.eval(RED_LOCK_SCRIPT, ScriptOutputType.INTEGER, new String[]{resource}, sign)
                );
            }
        }
    }

    /**
     * determine whether it is a master-slave
     */
    private boolean doMasterSlaveLock(String lock, String resource, String sign, long maxLockMilliseconds) {
        try (StatefulConnection<String, String> conn = super.getPool().borrowObject()) {
            // Get the connection and execute redis command
            StatefulRedisConnection<String, String> redisConn = (StatefulRedisConnection<String, String>) conn;
            long beginTime = System.currentTimeMillis();
            String result = redisConn.sync().set(resource, sign, SetArgs.Builder.nx().px(maxLockMilliseconds));
            long endTime = System.currentTimeMillis();
            if (endTime - beginTime >= maxLockMilliseconds) {
                return false;
            }
            boolean success = "OK".equals(result);
            if (success) {
                // just for re-entrance
                Map<String, String> redLocks = this.redLockThreadLocal.get();
                if (redLocks == null) {
                    redLocks = new HashMap<>();
                    this.redLockThreadLocal.set(redLocks);
                }
                redLocks.put(lock, sign);
            }
            return success;
        } catch (Exception e) {
            throw new RedisLockException("failed to obtain redis connection", e);
        }
    }

    protected <R> R doSingleNodeCmd(String key, Function<RedisCommands<String, String>, R> cmd) {
        try (StatefulConnection<String, String> conn = super.getPool().borrowObject()) {
            StatefulRedisConnection<String, String> nodeConn;
            if (super.getClusterFlag()) {
                nodeConn = ((StatefulRedisClusterConnection<String, String>) conn).getConnection(this.getNodeId(key));
            } else {
                nodeConn = (StatefulRedisConnection<String, String>) conn;
            }
            return cmd.apply(nodeConn.sync());
        } catch (Exception e) {
            throw new RedisLockException("failed to obtain redis connection", e);
        }
    }

    public String getLockKey(String lock) {
        return String.format("{%s}_%s", RED_LOCK_HASH_TAG, lock);
    }
}
