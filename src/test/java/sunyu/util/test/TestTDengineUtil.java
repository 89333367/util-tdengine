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
