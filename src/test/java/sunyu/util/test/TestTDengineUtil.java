package sunyu.util.test;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.thread.ThreadUtil;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

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

    @Test
    void 删除frequent数据库中不是d_p_开头的表() {
        while (true) {
            String sql = "select table_name from information_schema.ins_tables where db_name='frequent' and stable_name='d_p' and table_name not like 'd_p_%' limit 10000";
            List<Map<String, Object>> l = tDengineUtil.querySql(sql);
            if (CollUtil.isNotEmpty(l)) {
                for (Map<String, Object> m : l) {
                    String tableName = (String) m.get("table_name");
                    log.info("找到表名： {}", tableName);

                    // todo 删除表
                    String dropTableSql = "drop table frequent.`" + tableName + "`";
                    tDengineUtil.executeSql(dropTableSql);
                }
            } else {
                break;
            }
        }
        log.info("done");
    }


    @Test
    void 补录() throws InterruptedException {
        int i = 5;
        String pre = "d_p_";
        while (true) {
            // todo 查询 frequent 数据库中，不是 d_p_ 开头的表名
            String sql1 = "select table_name from information_schema.ins_tables where db_name='frequent' and stable_name='d_p' and table_name not like 'd_p_%' limit " + i;
            List<Map<String, Object>> l = tDengineUtil.querySql(sql1);
            if (CollUtil.isNotEmpty(l)) {
                CountDownLatch cdl = new CountDownLatch(l.size());
                for (Map<String, Object> m : l) {
                    ThreadUtil.execute(() -> {
                        try {
                            String tableName = (String) m.get("table_name");
                            log.info("找到表名： {}", tableName);

                            // todo 复制表
                            String sql2 = "insert into frequent.`" + pre + tableName + "` select * from frequent.`" + tableName + "`";
                            tDengineUtil.executeSql(sql2);

                            // todo 删除表
                            String sql3 = "drop table frequent.`" + tableName + "`";
                            tDengineUtil.executeSql(sql3);
                        } finally {
                            cdl.countDown();
                        }
                    });
                }
                cdl.await();
                log.info("sub done");
            } else {
                // todo 如果没有返回了，说明没有非 d_p_ 开头的表了
                log.info("没有非 d_p_ 开头的表了");
                break;
            }
            break;
        }
        log.info("done");
    }

}
