package com.supercode.infra.cache.spring;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * HelloRedis Configuration class
 */
@Configuration
public class HelloRedisConfiguration {

    @Bean(name = "helloRedis")
    public String helloRedis() {
        return "hello redis";
    }
}
