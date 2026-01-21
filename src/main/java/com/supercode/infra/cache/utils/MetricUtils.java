package com.supercode.infra.cache.utils;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
public final class MetricUtils {

    public static void recordTime(String name, String desc, Duration duration, String... tags) {
        try {
            builderTimer(name, desc, tags).record(duration);
        } catch (Exception e) {
            log.error("MetricsUtils.recordTime error.", e);
        }
    }

    private static Timer builderTimer(String name, String desc, String... tags) {
        return Timer.builder(name)
                .description(desc)
                .tags(tags)
                .register(Metrics.globalRegistry);
    }
}
