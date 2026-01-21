package com.supercode.infra.cache.constant;

/**
 * 客户端相关的参数
 *
 * @author jonathan.ji
 */
public class RedisClientConstant {

    private RedisClientConstant() {
    }

    public static final int MAX_TOTAL = 20;
    public static final int MAX_IDLE = 20;
    public static final int MIN_IDLE = 10;
    public static final long TIME_BETWEEN_EVICTION_RUNS_MILLIS = 30000;
    public static final long MAX_WAIT_MILLIS = 300;

    public static final String REDIS_URL = "supercode.redis.address";
    public static final String REDIS_PORT = "supercode.redis.port";
    public static final String REDIS_MAX_TOTAL = "supercode.redis.max.total";
    public static final String REDIS_MAX_IDLE = "supercode.redis.max.idle";
    public static final String REDIS_MIN_IDLE = "supercode.redis.min.idle";
    public static final String REDIS_CLUSTER_FLAG = "supercode.redis.cluster.flag";
    public static final String REDIS_PREPARE_POOL = "supercode.redis.prepare.pool";
}
