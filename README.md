# TDengine工具类

## 描述

在大数据环境中，TDengine的写入非常缓慢，
不是因为TDengine接收不了或性能不行，是因为写入的方式决定了写入的速度，
即使是使用了 Statement 的 addBatch() 和 executeBatch() 来批量执行，
也没有带来性能上的提升，
原因是 TDengine 的 JDBC 实现中，通过 addBatch 方法提交的 SQL 语句，
会按照添加的顺序，依次执行，这种方式没有减少与服务端的交互次数，不会带来性能上的提升，
所以本工具类就产生了，使用本工具类来减少服务器交互的次数，提升写入性能；

## 实现功能

1. 数据写入
    - 单条写入
    - 批量写入
2. 数据查询
    - ResultSet回调查询
    - List<Map<String,Object>>结果查询

## 特色

1. 工具类使用枚举类型编写，全局唯一，整个APP只需要初始化一次，避免过多实例浪费资源
2. 写入接口调用简单，只需调用insertRow即可，sql缓存、拼装与执行由工具类帮助完成
3. 数据写入使用了缓存，会自动将多条sql拼装成一条sql，利用多线程、多队列功能批量写入，实现快速写入
4. 数据写入一定成功，内部拥有重试机制，不会丢失写入数据
5. 数据查询开放回调接口，可以自定义处理结果集
6. 数据查询可返回List集合，操作简单

## 环境

* jdk8 x64 及以上版本
* TDengine 3.3.2.2 及以上版本

## 必要配置

### pom.xml(选择匹配jdk版本的依赖)

```xml
<!-- 工具类依赖，已经包含taos-jdbcdriver依赖，切记不要再引入taos-jdbcdriver依赖避免重复 -->
<dependency>
    <groupId>sunyu.util</groupId>
    <artifactId>util-tdengine</artifactId>
    <version>3.3.0_v1.0</version>
</dependency>
```

### 建议使用的数据源依赖

```xml
<!-- jdk11使用这个 -->
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>5.1.0</version>
</dependency>
```

```xml
<!-- jdk8使用这个 -->
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>4.0.3</version>
</dependency>
```

### 建议的数据源配置

```properties
# 数据源配置
spring.datasource.driver-class-name=com.taosdata.jdbc.rs.RestfulDriver
spring.datasource.url=jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true
spring.datasource.username=root
spring.datasource.password=taosdata
spring.datasource.hikari.minimum-idle=1
spring.datasource.hikari.maximum-pool-size=10
```

```properties
# 动态数据源配置
spring.datasource.dynamic.primary=tdengine
spring.datasource.dynamic.datasource.tdengine.driver-class-name=com.taosdata.jdbc.rs.RestfulDriver
spring.datasource.dynamic.datasource.tdengine.url=jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true
spring.datasource.dynamic.datasource.tdengine.username=root
spring.datasource.dynamic.datasource.tdengine.password=taosdata
spring.datasource.dynamic.hikari.min-idle=1
spring.datasource.dynamic.hikari.maximum-pool-size=10
```

> 如果配置 messageWaitTimeout 属性，那么不要配置时间太长，因为数据写入出现异常，会等待大于 messageWaitTimeout
> 的时间后重试，这里建议默认不配置即可

## API

```java
// 用于普通插入查询
// 更新类语句
public int executeUpdate(String sql, Integer retry, Integer sleepMillis)

// 查询统计类语句
public void executeQuery(String sql, ResultSetCallback callback)

public List<Map<String, Object>> executeQuery(String sql)

// 用于并发写入
// 写入数据
public void insertRow(String databaseName, String superTable, String tableName, TreeMap<String, Object> row)

// 可以一次写入一个多行数据
public void insertRow(String databaseName, String superTable, String tableName, Map<String, Object> row)

// 等待缓存写入
public void awaitExecution()

// 回收工具类资源，回收后工具类不可再使用
public void close()
```

## 示例

### 批量写入Demo

```java

@Test
void t001() {
    //数据源
    HikariConfig config = new HikariConfig();
    config.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
    config.setJdbcUrl("jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true");
    config.setUsername("root");
    config.setPassword("taosdata");
    DataSource dataSource = new HikariDataSource(config);

    //初始化，应用全局只需要初始化一个即可
    TDengineUtil tdUtil = TDengineUtil.builder().dataSource(dataSource).maxPoolSize(5).build();

    //多线程，模拟N张表并发写入
    Date d = DateUtil.parse("2023-01-01");
    IntStream.rangeClosed(1, 50).parallel().forEach(i -> {
        for (int j = 1; j <= 1000000; j++) {//每个表N行记录
            //写入一条记录
            int finalJ = j;
            tdUtil.insertRow("testdb2", "t", "t_" + i, new TreeMap<String, Object>() {{
                //这里可以直接写列和TAG
                put("c1", DateUtil.offsetSecond(d, finalJ).toString("yyyy-MM-dd HH:mm:ss"));
                put("c2", null);
                put("t1", DateUtil.offsetSecond(d, finalJ).toString("yyyy-MM-dd HH:mm:ss"));
            }});
        }
        tdUtil.awaitExecution();//等待一批缓存写入完毕
    });
    tdUtil.close();//资源回收
}
```

### 执行sql Demo

```java
tdUtil.executeUpdate("sql语句");

//这里可以传递重试次数和重试间隔时间
tdUtil.executeUpdate("sql语句",2,1000);
```

### 查询sql Demo

```java

@Test
void t002() {
    //数据源
    HikariConfig config = new HikariConfig();
    config.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
    config.setJdbcUrl("jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true");
    config.setUsername("root");
    config.setPassword("taosdata");
    DataSource dataSource = new HikariDataSource(config);

    //初始化，应用全局只需要初始化一个即可
    TDengineUtil tdUtil = TDengineUtil.builder().dataSource(dataSource).maxPoolSize(5).build();

    //查询可以自己使用resultSet回调，自己处理更灵活
    tdUtil.executeQuery("select * from testdb2.t limit 10", resultSet -> {
        while (resultSet.next()) {
            log.info("{}", resultSet.getTimestamp("c1"));
            log.info("{}", resultSet.getInt("c2"));
        }
    });

    //也可以使用封装好的查询方式返回列表，更简单
    List<Map<String, Object>> rows = tdUtil.executeQuery("select * from testdb2.t limit 10");
    for (Map<String, Object> row : rows) {
        log.info("{}", row);
    }

    //查询统计信息
    Map<String, Object> m = tdUtil.executeQuery("select count(*) total from testdb2.t").get(0);
    log.info("{}", m.get("total"));

    //查询其他信息
    for (Map<String, Object> r : tdUtil.executeQuery("desc testdb2.t")) {
        log.info("{}", r);
    }
}
```

### 解决科学计数法

```java
//如果是double返回值，有可能会出现科学计数法，可以使用下面方式转成字符串
new BigDecimal((Double)row.get("columnName")).toPlainString()
```

## Spark Demo

### pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>uml.tech</groupId>
    <artifactId>work-support</artifactId>
    <version>1.0.0</version>
    <name>work-support</name>
    <description>work-support</description>

    <properties>
        <java.version>11</java.version>
    </properties>

    <dependencies>
        <!-- https://mvnrepository.com/artifact/cn.hutool/hutool-all -->
        <!-- https://central.sonatype.com/artifact/cn.hutool/hutool-all/versions -->
        <dependency>
            <groupId>cn.hutool</groupId>
            <artifactId>hutool-all</artifactId>
            <version>5.8.29</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-core -->
        <!--<dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.10</artifactId>
            <version>1.6.3</version>
            <scope>provided</scope>
        </dependency>-->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.10</artifactId>
            <version>1.6.0-cdh5.9.0</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>sunyu.util</groupId>
            <artifactId>util-tdengine</artifactId>
            <version>3.3.0_v1.0</version>
        </dependency>
        <dependency>
            <groupId>com.zaxxer</groupId>
            <artifactId>HikariCP</artifactId>
            <version>5.1.0</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.7.1</version>
                <configuration>
                    <descriptors>
                        <descriptor>src/main/assembly/package.xml</descriptor>
                    </descriptors>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

### application.properties

```properties
# env
spring.profiles.active=dev
```

### application-dev.properties

```properties
spring.datasource.driver-class-name=com.taosdata.jdbc.rs.RestfulDriver
spring.datasource.url=jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true
spring.datasource.username=root
spring.datasource.password=taosdata
spring.datasource.hikari.minimum-idle=1
spring.datasource.hikari.maximum-pool-size=10
```

### spring-tdutil.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:context="http://www.springframework.org/schema/context"
       xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd http://www.springframework.org/schema/context https://www.springframework.org/schema/context/spring-context.xsd">
    <!-- 这里可以加载多个配置文件 -->
    <context:property-placeholder location="classpath:application.properties,classpath:application-dev.properties"/>

    <!-- 配置数据源 -->
    <bean id="dataSource" class="com.zaxxer.hikari.HikariDataSource" destroy-method="close">
        <property name="driverClassName" value="${spring.datasource.driver-class-name}"/>
        <property name="jdbcUrl" value="${spring.datasource.url}"/>
        <property name="username" value="${spring.datasource.username}"/>
        <property name="password" value="${spring.datasource.password}"/>
        <property name="minimumIdle" value="${spring.datasource.hikari.minimum-idle}"/>
        <property name="maximumPoolSize" value="${spring.datasource.hikari.maximum-pool-size}"/>
    </bean>
</beans>
```

### lockback.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="CONSOLE" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <root level="DEBUG">
        <appender-ref ref="CONSOLE"/>
    </root>
</configuration>
```

### 测试类

```java
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import uml.tech.bigdata.sdktdengine.util.TDengineUtil;

import javax.sql.DataSource;
import java.util.Date;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Tests {
    static Log log = LogFactory.get();
    static ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath*:spring-*.xml");
    //全局只需要创建一个
    static DataSource dataSource = applicationContext.getBean(DataSource.class);
    static TDengineUtil tdUtil = TDengineUtil.builder().dataSource(dataSource).maxPoolSize(5).build();

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark Local Example").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        //模拟 N 个设备
        JavaRDD<Integer> rdd = sparkContext.parallelize(IntStream.range(0, 50).boxed().collect(Collectors.toList()));
        rdd.foreachPartition((VoidFunction<Iterator<Integer>>) partitionIterator -> {
            log.info("分区 {} 执行开始", TaskContext.getPartitionId());
            Date d = DateUtil.parse("2023-01-01");
            //循环分区
            partitionIterator.forEachRemaining(i -> {
                String tableName = StrUtil.format("t_{}", i);
                String vin = StrUtil.format("t_v_{}", i);
                //每设备模拟 N 条数据
                for (int j = 0; j < 10; j++) {
                    int finalJ = j;
                    tdUtil.insertRow("testdb2", "t", tableName, new TreeMap<String, Object>() {{
                        put("c1", DateUtil.offsetSecond(d, finalJ).toString("yyyy-MM-dd HH:mm:ss"));//这里time可以是long类型或者任意日期时间格式的字符串
                        // 其他列信息 ...
                        // 如果是日期时间类型，可以传递 long
                        // 如果是日期时间类型，可以传递 "yyyy-MM-dd"
                        // 如果是日期时间类型，可以传递 "yyyy-MM-dd HH:mm:ss"
                        // 如果是日期时间类型，可以传递 "yyyy-MM-dd HH:mm:ss.000"
                        // 如果是高精度的ts，传递 "yyyy-MM-dd HH:mm:ss.000000"
                        put("t10", vin);//这里可以直接写tag的名称和值
                        // 其他TAG信息 ...
                    }});
                }
            });
            //分区执行完毕要调用awaitExecution方法，将剩余sql语句写入完毕
            tdUtil.awaitExecution();
            log.info("分区 {} 执行完毕", TaskContext.getPartitionId());
        });
        //不再使用工具类了，进行资源回收
        tdUtil.close();
        sparkContext.stop();
    }
}
```


