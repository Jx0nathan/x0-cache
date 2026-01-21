package com.supercode.infra.cache.spring;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(value = HelloRedisConfiguration.class)
public @interface EnableHelloRedisConfiguration {

}
