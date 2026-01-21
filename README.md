## 📚简介

封装缓存相关的功能，提供便捷的操作方式和监控

## 📦安装

### 🍊Maven

在项目的pom.xml的dependencies中加入以下内容:

```xml

<dependency>
    <groupId>com.supercode.infra.avengers</groupId>
    <artifactId>supercode-infra-cache</artifactId>
    <version>x.y.z</version>
</dependency>
```

### 🎋版本说明

| 版本号   | 功能                       |
|-------|--------------------------|
| 2.0.0 | 提供基本的Redis Command命令     |
| 2.1.7 | 提供加锁后执行function并且主动释放锁功能 |
| 2.1.8 | 提供布隆过滤器功能                |

------

### 🧬如何使用

#### 初始化 : 在启动类中增加Redis Client的初始化动作: @EnableSupercodeCache

    @EnableSupercodeCache
    public class DemoApplication {
        public static void main(String[] args) {
            System.setProperty(Constant.LOCAL_IP, IPUtils.getIp());
            SpringApplication.run(DemoApplication.class);
        }
    }

#### 增加配置 :

| 配置key                        | 功能                                                                             | 
|------------------------------|--------------------------------------------------------------------------------|
| supercode.redis.address      | redis的地址 (tf-usa-qa-common-cluster.kdavic.clustercfg.use1.cache.amazonaws.com) |
| supercode.redis.port         | redis的端口 (6379)                                                                
| supercode.redis.max.total    | 最大连接数，默认是20                                                                    |
| supercode.redis.max.idle     | 最大活跃数，默认是20                                                                    |
| supercode.redis.min.idle     | 最小活跃数，默认是10                                                                    |
| supercode.redis.cluster.flag | 是否集群，默认是true                                                                   |
| supercode.redis.prepare.pool | 是否提前准备线程池，默认是false                                                             |

#### step1 : 从Spring的上下文中获取SupercodeRedisClient

    @Resource
    private SupercodeRedisClient supercodeRedisClient;

#### step2.1 : 基于supercodeRedisClient选择业务需要使用到的数据结构

    @Resource
    private SupercodeRedisClient supercodeRedisClient;

    public void demo {
       supercodeRedisClient.redisStringCmd().set("testKey", "testValue");
       String testKey = supercodeRedisClient.redisStringCmd().get("testKey");
    }

| 数据结构                                     | 功能               |
|------------------------------------------|------------------|
| supercodeRedisClient.redisStringCmd()    | 字符串的相关操作         |
| supercodeRedisClient.redisListCmd()      | 数组的相关操作          |
| supercodeRedisClient.redisHashCmd()      | 哈希的相关操作          |
| supercodeRedisClient.redisGeoCmd()       | 地理位置的相关操作        |
| supercodeRedisClient.redisHllCmd()       | HyperLogLog的相关操作 |
| supercodeRedisClient.transactionCmd()    | 事务的相关操作          |
| supercodeRedisClient.redisSetCmd()       | Set的相关操作         |
| supercodeRedisClient.redisSortedSetCmd() | SortedSet的相关操作   |
| supercodeRedisClient.redisKeyCmd()       | Key的相关操作         |
| supercodeRedisClient.redisLockCmd()      | 分布式锁的相关操作        |

#### step2.2 : 基于supercodeRedisClient实现分布式锁

    @Resource
    private SupercodeRedisClient supercodeRedisClient;

    public void redisLock {
       boolean tryLockResult = supercodeRedisClient.redisLockCmd().tryRedLock(redisKey, 60 * 60 * 1000);
       if(tryLockResult){
         try{
           // do your business
         }finally{
           // release your lock
           supercodeRedisClient.redisLockCmd().releaseRedLock(redisKey);
         }
       }
    }