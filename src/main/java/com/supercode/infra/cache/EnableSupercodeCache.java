package com.supercode.infra.cache;

import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 启动缓存
 *
 * @author jonathan.ji
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Import({EnableCacheImportSelector.class})
public @interface EnableSupercodeCache {
}
