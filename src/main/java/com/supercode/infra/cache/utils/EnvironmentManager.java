package com.supercode.infra.cache.utils;

import org.springframework.core.env.ConfigurableEnvironment;

public class EnvironmentManager {

    /**
     * 通过Key 获取一个配置信息 优先读取用户的配置
     *
     * @param env 用户的环境配置类
     * @param key 需要获取的属性KEY
     * @return 返回一个配置信息, 如果找不到则为空
     */
    public static String getProperty(ConfigurableEnvironment env, String key) {
        if (env.getProperty(key) != null) {
            return env.getProperty(key);
        }
        return null;
    }
}
