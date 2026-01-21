package com.supercode.infra.cache.spring;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@EnableHelloRedisSelectorRegister
public class RedisSpringIntegratedTest03 {

    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        // 注册 Configuration Class
        context.register(RedisSpringIntegratedTest03.class);
        context.refresh();

        String helloRedis = context.getBean("helloRedis", String.class);
        System.out.println(helloRedis);

        // 关闭 Spring 应用上下文
        context.close();
    }
}
