# TDengine工具类

## 环境

* jdk8 x64 及以上版本
* TDengine 请根据数据库版本切换驱动版本

## 必要配置

### pom.xml

```xml
<dependency>
   <groupId>sunyu.util</groupId>
   <artifactId>util-tdengine</artifactId>
    <!-- {taos-jdbcdriver.version}_{util.version}_{jdk.version}_{architecture.version} -->
    <version>3.8.1_2.0_jdk8_x64</version>
   <classifier>shaded</classifier>
</dependency>
```

### 如果使用mybatis，那么添加这个配置
```xml
<dependency>
    <groupId>com.taosdata.jdbc</groupId>
    <artifactId>taos-jdbcdriver</artifactId>
    <version>3.8.1.fix.2.0.us.shaded</version>
    <classifier>shaded</classifier>
    <optional>true</optional>
</dependency>
```

### 建议使用的数据源依赖

```xml
<!-- jdk11使用这个 -->
<dependency>
    <groupId>com.zaxxer</groupId>
    <artifactId>HikariCP</artifactId>
    <version>7.0.2</version>
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
spring.datasource.url=jdbc:TAOS-WS://192.168.13.111:6041,192.168.13.112:6041,192.168.13.113:6041/?varcharAsString=true
spring.datasource.username=root
spring.datasource.password=taosdata
spring.datasource.hikari.minimum-idle=0
spring.datasource.hikari.maximum-pool-size=10
```

```properties
# 动态数据源配置
spring.datasource.dynamic.primary=tdengine
spring.datasource.dynamic.datasource.tdengine.driver-class-name=com.taosdata.jdbc.ws.WebSocketDriver
spring.datasource.dynamic.datasource.tdengine.url=jdbc:TAOS-WS://192.168.13.111:6041,192.168.13.112:6041,192.168.13.113:6041/?varcharAsString=true
spring.datasource.dynamic.datasource.tdengine.username=root
spring.datasource.dynamic.datasource.tdengine.password=taosdata
spring.datasource.dynamic.hikari.min-idle=0
spring.datasource.dynamic.hikari.maximum-pool-size=10
```

> 如果配置 messageWaitTimeout 属性，那么不要配置时间太长，因为数据写入出现异常，会等待大于 messageWaitTimeout 的时间后重试，这里建议默认值即可


## 示例

```java
package sunyu.util.test;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateTime;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.setting.dialect.Props;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import sunyu.util.TDengineUtil;
import sunyu.util.test.config.ConfigProperties;

import java.util.HashMap;
import java.util.Map;

public class TestTDengineUtil {
    Log log = LogFactory.get();

    static Props props = ConfigProperties.getProps();
    static TDengineUtil tDengineUtil;

    @BeforeAll
    static void beforeClass() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(props.getStr("driverClassName"));
        config.setJdbcUrl(props.getStr("jdbcUrl"));
        config.setUsername(props.getStr("username"));
        config.setPassword(props.getStr("password"));
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(10);
        HikariDataSource ds = new HikariDataSource(config);
        tDengineUtil = TDengineUtil.builder().dataSource(ds).setMaxSqlLength(1024 * 1024).setShowSql(true).build();
    }

    @Test
    void t001() {
        tDengineUtil.insert("frequent", "d_p", "test", new HashMap<String, Object>() {{
            put("3014", "2026-01-21 00:00:00");
            put("protocol", "xxx");
            put("did", "test");
        }});
    }

    @Test
    void t002() {
        DateTime dt = new DateTime("2026-01-20 00:00:00");
        for (int i = 0; i < 80000; i++) {
            tDengineUtil.appendInsert("frequent", "d_p", "test", new HashMap<String, Object>() {{
                put("3014", dt.offset(DateField.SECOND, 1));
                put("protocol", "xxx");
                put("did", "test");
            }});
        }
        tDengineUtil.await();
    }

    @Test
    void t003() {
        for (Map<String, Object> showDatabases : tDengineUtil.querySql("show databases")) {
            log.info("show databases: " + showDatabases);
        }
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