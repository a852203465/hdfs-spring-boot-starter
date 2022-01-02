# hdfs-spring-boot-starter
    封装 hdfs 并制作Spring starter

## 使用方式

1. 下载源码

下载源码 并install引入使用

2. 引入依赖

```xml
<dependency>
    <groupId>cn.darkjrong</groupId>
    <artifactId>hdfs-spring-boot-starter</artifactId>
    <version>1.0</version>
</dependency>
```

3. 配置参数(application.properties)  yml配置

```yaml
hdfs:
  enabled: true # 必须为true, 才能生效
  namespace: /data
  replication: 1
  username: Mr.J
  server-address: hdfs://localhost:9000
```
4. API 注入
```java

@Autowired
private HdfsTemplate hdfsTemplate;


```











