package com.supercode.infra.cache.redis.client;

import com.supercode.infra.cache.constant.RedisClientConstant;
import com.supercode.infra.cache.lock.RedisLock;
import com.supercode.infra.cache.redis.cmd.*;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SocketOptions;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.metrics.MicrometerCommandLatencyRecorder;
import io.lettuce.core.metrics.MicrometerOptions;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.support.ConnectionPoolSupport;
import io.micrometer.core.instrument.Metrics;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.io.Closeable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * @author jonathan.ji
 */
@Log4j2
public class SupercodeRedisClient<K, V> implements Closeable {

    public boolean isCluster = false;
    private RedisClient client;
    private RedisClusterClient clusterClient;

    private volatile boolean closing = false;

    private ThreadLocal<StatefulConnection<String, String>> transactionConn = new ThreadLocal<>();
    public ThreadLocal<RedisCommands<String, String>> transactionCmd = new ThreadLocal<>();

    private final RedisGeoCmd<K, V> redisGeoCmd = new RedisGeoCmd<>(this);
    private final RedisStringCmd<K, V> redisStringCmd = new RedisStringCmd<>(this);
    private final RedisListCmd<K, V> redisListCmd = new RedisListCmd<>(this);
    private final RedisHashCmd<K, V> redisHashCmd = new RedisHashCmd<>(this);
    private final RedisHllCmd<K, V> redisHllCmd = new RedisHllCmd<>(this);
    private final RedisSetCmd<K, V> redisSetCmd = new RedisSetCmd<>(this);
    private final RedisSortedSetCmd<K, V> redisSortedSetCmd = new RedisSortedSetCmd<>(this);
    private final RedisKeyCmd<K, V> redisKeyCmd = new RedisKeyCmd<>(this);
    private final RedisLock<K, V> redisLockCmd = new RedisLock<>(this);
    private final RedisTransactionCmd<K, V> redisTransactionCmd = new RedisTransactionCmd<>(this, transactionConn, transactionCmd);


    /**
     * Connection pooling with Lettuce can be required when you're invoking Redis operations in multiple threads and you use
     * (1) blocking commands such as BLPOP
     * (2) transactions BLPOP.
     * (3) command batching
     */
    public GenericObjectPool<StatefulConnection<String, String>> pool;

    public SupercodeRedisClient(String uri, int port) {
        this(uri, port, true, true);
    }

    public SupercodeRedisClient(String uri, int port, boolean isCluster, boolean readFromMaster) {
        this(uri, port, RedisClientConstant.MAX_TOTAL, RedisClientConstant.MAX_IDLE, RedisClientConstant.MIN_IDLE, isCluster, readFromMaster, false);
    }

    public SupercodeRedisClient(String uri, int port, Integer maxTotal, Integer maxIdle, Integer minIdle,
                                boolean isCluster, boolean preparePool) {
        this(uri, port, maxTotal, maxIdle, minIdle, isCluster, true, preparePool);
    }

    /**
     * @param uri            redis的连接URL
     * @param port           redis的端口号
     * @param maxTotal       最大连接数，我们当前Redis的单机最大连接数是65K
     * @param maxIdle        连接池允许的最大空闲数，避免连接池伸缩导致的性能问题，建议与maxTotal一致
     * @param minIdle        最小连接数
     * @param isCluster      是否使用Cluster集群模式，true-使用cluster
     * @param readFromMaster 是否从主节点读取数据
     * @param preparePool    是否初始化线程池
     */
    private SupercodeRedisClient(String uri, int port, Integer maxTotal, Integer maxIdle, Integer minIdle,
                                 boolean isCluster, boolean readFromMaster, boolean preparePool) {
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(maxTotal);
        poolConfig.setMaxIdle(maxIdle);
        poolConfig.setMinIdle(minIdle);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRunsMillis(RedisClientConstant.TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        poolConfig.setMaxWaitMillis(RedisClientConstant.MAX_WAIT_MILLIS);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setJmxEnabled(true);
        poolConfig.setJmxNamePrefix("supercode-redis-pool");

        this.init(uri, port, poolConfig, isCluster, readFromMaster, preparePool);
    }

    private void init(String uri, int port, GenericObjectPoolConfig poolConfig,
                      boolean isCluster, boolean readFromMaster, boolean preparePool) {
        // client options
        ClusterClientOptions options = ClusterClientOptions.builder()
                .socketOptions(this.wrapperSocketOptions())
                .validateClusterNodeMembership(false)
                .topologyRefreshOptions(this.wrapperClusterTopologyRefreshOptions())
                .build();

        RedisURI redisUri = new RedisURI(uri, port, Duration.of(10, ChronoUnit.SECONDS));
        ClientResources resources = ClientResources.builder().commandLatencyRecorder(new MicrometerCommandLatencyRecorder(Metrics.globalRegistry, MicrometerOptions.create())).build();

        // create redis or redis cluster client
        if (isCluster) {
            this.isCluster = true;
            this.clusterClient = RedisClusterClient.create(resources, redisUri);
            this.clusterClient.setOptions(options);
        } else {
            this.isCluster = false;
            this.client = RedisClient.create(resources, redisUri);
            this.client.setOptions(options);
        }

        // init redis connection pool
        this.pool = ConnectionPoolSupport.createGenericObjectPool(this.isCluster ? () -> {
                    StatefulRedisClusterConnection<String, String> conn = clusterClient.connect();
                    if (readFromMaster) {
                        conn.setReadFrom(ReadFrom.MASTER);
                    } else {
                        conn.setReadFrom(ReadFrom.SLAVE_PREFERRED);
                    }
                    return conn;
                } : () -> this.client.connect(), poolConfig
        );
        if (preparePool) {
            try {
                log.info("supercode redis init prepare pool");
                this.pool.preparePool();
            } catch (Exception e) {
                log.error("prepare pool error.", e);
            }
        }
    }

    private SocketOptions wrapperSocketOptions() {
        return SocketOptions.builder()
                .connectTimeout(Duration.of(2, ChronoUnit.SECONDS))
                .keepAlive(true)
                .tcpNoDelay(true)
                .build();
    }

    /**
     * https://github.com/lettuce-io/lettuce-core/issues/339
     */
    private ClusterTopologyRefreshOptions wrapperClusterTopologyRefreshOptions() {
        return ClusterTopologyRefreshOptions.builder()
                // 是否允许周期性更新集群拓扑视图
                .enablePeriodicRefresh(true)
                // 更新集群拓扑视图周期-5s
                .refreshPeriod(Duration.of(5, ChronoUnit.SECONDS))
                .enableAllAdaptiveRefreshTriggers()
                .build();
    }

    public RedisGeoCmd<K, V> redisGeoCmd() {
        return redisGeoCmd;
    }

    /**
     * 不带过期时间的过滤器
     *
     * @param filterName         过滤器名称
     * @param expectedInsertions 预期插入的元素数量
     * @param errorRate          误差率（误差率越小性能相对越差，误差率的设置可根据自己业务情况设定）
     * @return
     */
    public RedisBloomFilterCmd<String, String> redisBloomFilterCmd(String filterName,
                                                                   long expectedInsertions, double errorRate) {
        RedisBloomFilterCmd<String, String> redisBFCmd =
                new RedisBloomFilterCmd(expectedInsertions, errorRate, filterName, 0L, this);
        return redisBFCmd;
    }

    /**
     * 不带误差率的过滤器
     *
     * @param filterName         过滤器名称
     * @param expectedInsertions 预期插入的元素数量
     * @return
     */
    public RedisBloomFilterCmd<String, String> redisBloomFilterCmd(String filterName,
                                                                   long expectedInsertions) {
        RedisBloomFilterCmd<String, String> redisBFCmd =
                new RedisBloomFilterCmd(expectedInsertions, 0.03, filterName, 0L, this);
        return redisBFCmd;
    }

    /**
     * 带过期时间的过滤器
     *
     * @param filterName         过滤器名称
     * @param expectedInsertions 预期插入的元素数量
     * @param errorRate          误差率（误差率越小性能相对越差，误差率的设置可根据自己业务情况设定）
     * @param expiration         过期时间（单位毫秒）
     * @return
     */
    public RedisBloomFilterCmd<String, String> redisBloomFilterCmd(String filterName,
                                                                   long expectedInsertions, double errorRate, long expiration) {
        RedisBloomFilterCmd<String, String> redisBFCmd =
                new RedisBloomFilterCmd(expectedInsertions, errorRate, filterName, expiration, this);
        return redisBFCmd;
    }

    public RedisStringCmd<K, V> redisStringCmd() {
        return redisStringCmd;
    }

    public RedisListCmd<K, V> redisListCmd() {
        return redisListCmd;
    }

    public RedisHashCmd<K, V> redisHashCmd() {
        return redisHashCmd;
    }

    public RedisHllCmd<K, V> redisHllCmd() {
        return redisHllCmd;
    }

    public RedisTransactionCmd<K, V> transactionCmd() {
        return redisTransactionCmd;
    }

    public RedisSetCmd<K, V> redisSetCmd() {
        return redisSetCmd;
    }

    public RedisSortedSetCmd<K, V> redisSortedSetCmd() {
        return redisSortedSetCmd;
    }

    public RedisKeyCmd<K, V> redisKeyCmd() {
        return redisKeyCmd;
    }

    public RedisLock<K, V> redisLockCmd() {
        return redisLockCmd;
    }

    public GenericObjectPool<StatefulConnection<String, String>> getPool() {
        return this.pool;
    }

    public boolean getCloseFlag() {
        return this.closing;
    }

    public boolean getClusterFlag() {
        return this.isCluster;
    }

    public RedisClusterClient getClusterClient() {
        return this.clusterClient;
    }

    @Override
    public void close() {
        this.closing = true;
        this.pool.close();
        if (this.isCluster) {
            this.clusterClient.shutdown();
        } else {
            this.client.shutdown();
        }
    }
}
