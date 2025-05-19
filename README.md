# TDengine工具类

## 环境

* jdk8 x64 及以上版本
* TDengine 请根据数据库版本切换驱动版本

## 必要配置

### pom.xml(选择匹配jdk版本的依赖)

```xml
<!-- 工具类依赖，未含taos-jdbcdriver依赖，需要自己引入合适版本的驱动 -->
<dependency>
   <groupId>sunyu.util</groupId>
   <artifactId>util-tdengine</artifactId>
   <!-- {util.version}_{jdk.version}_{architecture.version} -->
   <version>1.0_jdk8_x64</version>
</dependency>

<!-- https://central.sonatype.com/artifact/com.taosdata.jdbc/taos-jdbcdriver/versions -->
<dependency>
    <groupId>com.taosdata.jdbc</groupId>
    <artifactId>taos-jdbcdriver</artifactId>
    <!-- 数据库版本和驱动对应关系 -->
    <!-- https://docs.taosdata.com/reference/connector/java/ -->
    <version>3.4.0</version>
</dependency>
```

### 建议使用的数据源依赖

```xml
<!-- jdk11使用这个 -->
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>6.3.0</version>
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
spring.datasource.driver-class-name=com.taosdata.jdbc.ws.WebSocketDriver
spring.datasource.url=jdbc:TAOS-WS://192.168.13.87:16042/?httpConnectTimeout=60000&messageWaitTimeout=60000
#spring.datasource.driver-class-name=com.taosdata.jdbc.rs.RestfulDriver
#spring.datasource.url=jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true&httpConnectTimeout=60000&messageWaitTimeout=60000&httpPoolSize=20
spring.datasource.username=root
spring.datasource.password=taosdata
spring.datasource.hikari.minimum-idle=0
spring.datasource.hikari.maximum-pool-size=10
```

```properties
# 动态数据源配置
spring.datasource.dynamic.primary=tdengine
spring.datasource.dynamic.datasource.tdengine.driver-class-name=com.taosdata.jdbc.ws.WebSocketDriver
spring.datasource.dynamic.datasource.tdengine.url=jdbc:TAOS-WS://192.168.13.87:16042/?httpConnectTimeout=60000&messageWaitTimeout=60000
#spring.datasource.dynamic.datasource.tdengine.driver-class-name=com.taosdata.jdbc.rs.RestfulDriver
#spring.datasource.dynamic.datasource.tdengine.url=jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true&httpConnectTimeout=60000&messageWaitTimeout=60000&httpPoolSize=20
spring.datasource.dynamic.datasource.tdengine.username=root
spring.datasource.dynamic.datasource.tdengine.password=taosdata
spring.datasource.dynamic.hikari.min-idle=0
spring.datasource.dynamic.hikari.maximum-pool-size=10
```

> 如果配置 messageWaitTimeout 属性，那么不要配置时间太长，因为数据写入出现异常，会等待大于 messageWaitTimeout 的时间后重试，这里建议默认值即可


## 示例

```java
package sunyu.util.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import sunyu.util.TDengineUtil;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestTDengineUtil {
    Log log = LogFactory.get();

    public DataSource getDataSource() {
        //数据源
        HikariConfig config = new HikariConfig();
        //config.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
        //config.setJdbcUrl("jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true&httpConnectTimeout=60000&messageWaitTimeout=60000&httpPoolSize=20");
        config.setDriverClassName("com.taosdata.jdbc.ws.WebSocketDriver");
        config.setJdbcUrl("jdbc:TAOS-WS://192.168.13.87:16042/?httpConnectTimeout=60000&messageWaitTimeout=60000");
        config.setUsername("root");
        config.setPassword("taosdata");
        return new HikariDataSource(config);
    }


    @Test
    void createDatabase() {
        String sql = "CREATE DATABASE IF NOT EXISTS `db_test`";
        TDengineUtil tdengineUtil = TDengineUtil.builder().dataSource(getDataSource()).build();
        tdengineUtil.executeUpdate(sql);
        tdengineUtil.close();
    }

    @Test
    void createSTable() {
        String sql = "CREATE STABLE IF NOT EXISTS `db_test`.`stb_test` (c1 TIMESTAMP,c2 VARCHAR(100),c3 INT,c4 FLOAT) TAGS (t1 VARCHAR(50))";
        TDengineUtil tdengineUtil = TDengineUtil.builder().dataSource(getDataSource()).build();
        tdengineUtil.executeUpdate(sql);
        tdengineUtil.close();
    }

    @Test
    void insertRows() {
        TDengineUtil tdengineUtil = TDengineUtil.builder().dataSource(getDataSource()).build();

        for (int i = 0; i < 10; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("c1", "2025-03-25 13:18:00");
            row.put("c2", "value" + i);
            row.put("c3", i);
            row.put("c4", i * 1.0);
            row.put("t1", "tag" + i);
            tdengineUtil.insertRow("db_test", "stb_test", "tb_test" + i, row);
        }

        tdengineUtil.close();
    }

    @Test
    void insertRows2() {
        TDengineUtil tdengineUtil = TDengineUtil.builder().dataSource(getDataSource()).build();

        for (int i = 0; i < 10; i++) {
            Map<String, Object> rowValue = new HashMap<>();
            rowValue.put("c1", "2025-03-25 13:18:00");
            rowValue.put("c2", "value" + i);
            rowValue.put("c3", i);
            rowValue.put("c4", i * 1.0);
            Map<String, Object> tagValue = new HashMap<>();
            tagValue.put("t1", "tag" + i);
            tdengineUtil.insertRow("db_test", "stb_test", "tb_test" + i, rowValue, tagValue);
        }

        tdengineUtil.close();
    }

    @Test
    void asyncInsertRows() {
        TDengineUtil tdengineUtil = TDengineUtil.builder().dataSource(getDataSource())
                //设置并发数，默认10
                .setMaxConcurrency(10)
                .build();

        for (int i = 0; i < 10; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put("c1", "2025-03-26 13:18:00");
            row.put("c2", "value" + i);
            row.put("c3", i);
            row.put("c4", i * 1.0);
            row.put("t1", "tag" + i);
            tdengineUtil.asyncInsertRow("db_test", "stb_test", "tb_test" + i, row);//异步插入
        }
        tdengineUtil.awaitAllTasks();//等待所有任务完成

        tdengineUtil.close();
    }

    @Test
    void asyncInsertRows2() {
        TDengineUtil tdengineUtil = TDengineUtil.builder().dataSource(getDataSource())
                //设置并发数，默认10
                .setMaxConcurrency(10)
                .build();

        for (int i = 0; i < 10; i++) {
            Map<String, Object> rowValue = new HashMap<>();
            rowValue.put("c1", "2025-03-25 13:18:00");
            rowValue.put("c2", "value" + i);
            rowValue.put("c3", i);
            rowValue.put("c4", i * 1.0);
            Map<String, Object> tagValue = new HashMap<>();
            tagValue.put("t1", "tag" + i);
            tdengineUtil.asyncInsertRow("db_test", "stb_test", "tb_test" + i, rowValue, tagValue);//异步插入
        }
        tdengineUtil.awaitAllTasks();//等待所有任务完成

        tdengineUtil.close();
    }

    @Test
    void query() {
        //String sql = "SHOW DATABASES";
        //String sql = "SHOW CREATE DATABASE db_test";
        //String sql = "SHOW CREATE STABLE db_test.stb_test";
        //String sql = "DESC db_test.stb_test";
        String sql = "select * from db_test.stb_test limit 5";
        TDengineUtil tdengineUtil = TDengineUtil.builder().dataSource(getDataSource()).build();
        List<Map<String, Object>> rows = tdengineUtil.executeQuery(sql);
        log.info("查询结果: {}", rows);
        tdengineUtil.close();
    }


}

```

### 解决科学计数法

```java
//如果是double返回值，有可能会出现科学计数法，可以使用下面方式转成字符串
new BigDecimal((Double)row.get("columnName")).toPlainString()
```

## Spark Demo

[查看示例源码](https://github.com/89333367/demo-spark-hdfs-to-tdengine)


