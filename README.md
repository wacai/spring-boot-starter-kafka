## 依赖

```xml
<dependency>
    <groupId>com.wacai</groupId>
    <artifactId>spring-boot-starter-kafka</artifactId>
    <version>${version}</version>
</dependency>

```

> 最新版请见[nexus](http://repo.caimi-inc.com/nexus/#nexus-search;quick~spring-boot-starter-kafka)


## Kafka Producer Properties

* `bootstrap.servers` 是必须要有的配置项, 值是 kafka 的 broker 地址列表, 更多属性参见[官方文档](http://kafka.apache.org/documentation.html#producerconfigs)

```
springboot.kafka.conf.props[bootstrap.servers]=10.0.0.1:9092,10.0.0.2:9092

# 发送回执
springboot.kafka.conf.props[acks]=1

# 重试
springboot.kafka.conf.props[retries]=3 
springboot.kafka.conf.props[retry.backoff.ms]=1000

# 元数据
springboot.kafka.conf.props[metadata.fetch.timeout.ms]=5000 # 设置短一点时间有利于尽快发现配置异常

```

> `springboot.kafka.conf.props[bootstrap.servers]`的写法请参见[Spring-Boot-Configuration-Binding](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-Configuration-Binding)

其余的配置参见 [KafkaConfiguration](./src/main/java/com/wacai/springboot/kafka/autocfg/KafkaConfiguration.java)
