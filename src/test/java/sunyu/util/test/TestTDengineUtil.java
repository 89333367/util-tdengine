package sunyu.util.test;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import sunyu.util.TDengineUtil;

import javax.sql.DataSource;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestTDengineUtil {
    Log log = LogFactory.get();

    public DataSource getDataSource() {
        //数据源
        HikariConfig config = new HikariConfig();

        /*config.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
        config.setJdbcUrl("jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true&httpConnectTimeout=60000&messageWaitTimeout=60000&httpPoolSize=20");
        config.setUsername("root");
        config.setPassword("taosdata");*/

        /*config.setDriverClassName("com.taosdata.jdbc.ws.WebSocketDriver");
        config.setJdbcUrl("jdbc:TAOS-WS://192.168.13.87:16042/?httpConnectTimeout=60000&messageWaitTimeout=60000");
        config.setUsername("root");
        config.setPassword("taosdata");*/

        config.setDriverClassName("com.taosdata.jdbc.ws.WebSocketDriver");
        config.setJdbcUrl("jdbc:TAOS-WS://182.92.4.7:6041/?httpConnectTimeout=60000&messageWaitTimeout=60000");
        config.setUsername("bcuser");
        config.setPassword("Bcld&202509");

        /*config.setDriverClassName("com.taosdata.jdbc.ws.WebSocketDriver");
        config.setJdbcUrl("jdbc:TAOS-WS://172.16.1.173:16041/?httpConnectTimeout=60000&messageWaitTimeout=60000");
        config.setUsername("root");
        config.setPassword("taosdata");*/

        return new HikariDataSource(config);
    }

    @Test
    void dfStatistics() {
        TDengineUtil tdengineUtil = TDengineUtil.builder().dataSource(getDataSource()).build();
        String databaseName = "nrvp";
        String superTableName = "v_s";
        String vehicleId = "DF112041AP4H14313";
        String vehicleModel = "1204-1";
        String tableName = superTableName + "_" + vehicleId;
        // 计算 每日发动机工作时间
        String sql = StrUtil.format(ResourceUtil.readUtf8Str("workHours.sql"), vehicleId);
        log.debug("{}", sql);
        for (Map<String, Object> rows : tdengineUtil.executeQuery(sql)) {
            rows.put("vehicleId", vehicleId);
            rows.put("vehicleModel", vehicleModel);
            log.debug("{}", rows);
            tdengineUtil.asyncInsertRow(databaseName, superTableName, tableName, rows);
        }
        tdengineUtil.awaitAllTasks();//等待写入完毕
        // 计算 单车每日油耗 + 基于燃油消耗的能耗
        sql = StrUtil.format(ResourceUtil.readUtf8Str("fuelConsumption_energyByFuelConsumption.sql"), vehicleId);
        log.debug("{}", sql);
        for (Map<String, Object> rows : tdengineUtil.executeQuery(sql)) {
            rows.put("vehicleId", vehicleId);
            rows.put("vehicleModel", vehicleModel);
            log.debug("{}", rows);
            tdengineUtil.asyncInsertRow(databaseName, superTableName, tableName, rows);
        }
        tdengineUtil.awaitAllTasks();//等待写入完毕
        // 读取 统计规则
        tdengineUtil.close();
    }

    @Test
    void testResourceFile() {
        InputStream stream = ResourceUtil.getStream("统计规则.xlsx");
        ExcelReader reader = ExcelUtil.getReader(stream);
        List<Map<String, Object>> params = reader.readAll();
        log.debug("{}", params);
        reader.close();
    }


    @Test
    void killQuery() {
        String sql = "select kill_id,sql from performance_schema.perf_queries where sql like 'select last(% where vin in (%'";
        TDengineUtil tdengineUtil = TDengineUtil.builder().dataSource(getDataSource()).build();
        while (true) {
            List<Map<String, Object>> rows = tdengineUtil.executeQuery(sql);
            if (CollUtil.isEmpty(rows)) {
                break;
            }
            rows.forEach(row -> {
                String killId = row.get("kill_id").toString();
                String s = row.get("sql").toString();
                String killSql = StrUtil.format("kill query '{}'", killId);
                log.info("执行kill query: {} {}", killSql, s);
                tdengineUtil.executeUpdate(killSql);
            });
            ThreadUtil.sleep(1000 * 5);
        }
        tdengineUtil.close();
    }

    @Test
    void showDatabases() {
        TDengineUtil tdengineUtil = TDengineUtil.builder().dataSource(getDataSource()).build();
        log.info("{}", tdengineUtil.executeQuery("show databases"));
        tdengineUtil.close();
    }

    @Test
    void query2() {
        String sql = "select * from frequent.v_c limit 10";
        TDengineUtil tdengineUtil = TDengineUtil.builder().dataSource(getDataSource()).build();
        log.info("{}", tdengineUtil.executeQuery(sql));
        tdengineUtil.close();
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
