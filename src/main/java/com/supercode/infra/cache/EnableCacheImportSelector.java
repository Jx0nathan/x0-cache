package com.supercode.infra.cache;

import com.supercode.infra.cache.config.SupercodeRedisConfig;
import org.springframework.context.annotation.DeferredImportSelector;
import org.springframework.core.type.AnnotationMetadata;

/**
 * 控制缓存的使用
 *
 * @author jonathan.ji
 */
public class EnableCacheImportSelector implements DeferredImportSelector {

    @Override
    public String[] selectImports(AnnotationMetadata annotationMetadata) {
        return new String[]{SupercodeRedisConfig.class.getName()};
    }
}
