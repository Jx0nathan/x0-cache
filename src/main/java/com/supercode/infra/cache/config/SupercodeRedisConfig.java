package com.supercode.infra.cache.config;

import com.supercode.infra.cache.lock.RedisLock;
import com.supercode.infra.cache.redis.client.SupercodeRedisClient;
import com.supercode.infra.cache.utils.EnvironmentManager;
import com.supercode.master.utils.spring.register.AbstractBeanRegistrar;
import com.supercode.master.utils.spring.register.SupercodeBeanDefinition;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import java.util.Objects;

import static com.supercode.infra.cache.constant.RedisClientConstant.*;

/**
 * @author jonathan.ji
 */
@Log4j2
public class SupercodeRedisConfig extends AbstractBeanRegistrar {

    private String redisAddress;
    private int redisPort;
    private int maxTotal = MAX_TOTAL;
    private int maxIdle = MAX_IDLE;
    private int minIdle = MIN_IDLE;
    private boolean clusterFlag = true;
    private boolean preparePool = false;

    @Bean
    public RedisLock getRedisLock(@Autowired SupercodeRedisClient supercodeRedisClient) {
        return new RedisLock(supercodeRedisClient);
    }

    @Override
    public void registerBeans() {
        initConfig();
        log.info("SupercodeRedisConfig.start.init address is {} port is {} maxTotal is {} maxIdle is {} minIdle is {} ", redisAddress, redisPort, maxTotal, maxIdle, minIdle);
        registerBeanDefinitionIfNotExists(SupercodeBeanDefinition.newInstance(SupercodeRedisClient.class)
                .addConstructorArgValue(redisAddress, redisPort, maxTotal, maxIdle, minIdle, clusterFlag, preparePool)
                .setBeanName("SupercodeRedisClient"));
    }

    private void initConfig() {
        this.redisAddress = EnvironmentManager.getProperty(super.env, REDIS_URL);
        this.redisPort = Integer.parseInt(Objects.requireNonNull(EnvironmentManager.getProperty(env, REDIS_PORT)));

        String maxTotalStr = EnvironmentManager.getProperty(super.env, REDIS_MAX_TOTAL);
        if (maxTotalStr != null) {
            this.maxTotal = Integer.parseInt(maxTotalStr);
        }

        String maxIdleStr = EnvironmentManager.getProperty(super.env, REDIS_MAX_IDLE);
        if (maxIdleStr != null) {
            this.maxIdle = Integer.parseInt(maxIdleStr);
        }

        String minIdleStr = EnvironmentManager.getProperty(super.env, REDIS_MIN_IDLE);
        if (minIdleStr != null) {
            this.minIdle = Integer.parseInt(minIdleStr);
        }

        String redisClusterFlag = EnvironmentManager.getProperty(super.env, REDIS_CLUSTER_FLAG);
        if (redisClusterFlag != null) {
            this.clusterFlag = Boolean.parseBoolean(redisClusterFlag);
        }

        String redisPreparePool = EnvironmentManager.getProperty(super.env, REDIS_PREPARE_POOL);
        if (redisPreparePool != null) {
            this.preparePool = Boolean.parseBoolean(redisPreparePool);
        }
    }
}
