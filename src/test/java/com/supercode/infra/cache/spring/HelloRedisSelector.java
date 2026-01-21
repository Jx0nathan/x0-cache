package com.supercode.infra.cache.spring;

import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

/**
 * HelloRedis的ImportSelector实现
 */
public class HelloRedisSelector implements ImportSelector {

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        // 直接返回配置类
        return new String[]{"com.supercode.infra.cache.spring.HelloRedisConfiguration"};
    }
}
