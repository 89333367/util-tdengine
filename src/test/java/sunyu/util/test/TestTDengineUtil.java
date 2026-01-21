package sunyu.util.test;

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
        tDengineUtil = TDengineUtil.builder().dataSource(ds).setMaxConcurrency(10).setMaxSqlLength(1024 * 512).build();
    }

    @Test
    void t001() {
        tDengineUtil.asyncInsertRow("frequent", "d_p", "test", new HashMap<String, Object>() {{
            put("3014", "2026-01-21 00:00:00");
            put("protocol", "xxx");
            put("did", "test");
        }});
        tDengineUtil.awaitAllTasks();
    }

}
