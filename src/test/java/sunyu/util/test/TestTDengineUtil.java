package sunyu.util.test;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.resource.ResourceUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import sunyu.util.TDengineUtil;

import javax.sql.DataSource;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

public class TestTDengineUtil {
    Log log = LogFactory.get();

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
            for (int j = 1; j <= 10; j++) {//每个表N行记录
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

    @Test
    void t002() throws Exception {
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

    @Test
    void t003() throws Exception {
        //数据源
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
        config.setJdbcUrl("jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true");
        config.setUsername("root");
        config.setPassword("taosdata");
        DataSource dataSource = new HikariDataSource(config);

        //初始化，应用全局只需要初始化一个即可
        TDengineUtil tdUtil = TDengineUtil.builder().dataSource(dataSource).maxPoolSize(5).build();
        while (true) {
            List<Map<String, Object>> list = tdUtil.executeQuery("select table_name from information_schema.ins_tables where db_name='testdb2' limit 10000");
            if (CollUtil.isEmpty(list)) {
                break;
            }
            StringBuilder sb = new StringBuilder();
            sb.append("DROP TABLE");
            for (Map<String, Object> m : list) {
                String tableName = m.get("table_name").toString();
                log.info("{}", tableName);
                sb.append(StrUtil.format(" if exists testdb2.{},", tableName));
            }
            tdUtil.executeUpdate(StrUtil.strip(sb.toString(), ","), null, null);
        }
        tdUtil.executeUpdate("compact database testdb2", null, null);
        List<Map<String, Object>> compacts = tdUtil.executeQuery("show compacts");
        while (compacts.size() > 0) {
            for (Map<String, Object> compact : compacts) {
                log.info("{}", compact);
            }
            compacts = tdUtil.executeQuery("show compacts");
            ThreadUtil.sleep(1000);
        }
        log.info("done");
    }


    @Test
    void t004() throws Exception {
        //数据源
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
        config.setJdbcUrl("jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true");
        config.setUsername("root");
        config.setPassword("taosdata");
        DataSource dataSource = new HikariDataSource(config);

        //初始化，应用全局只需要初始化一个即可
        TDengineUtil tdUtil = TDengineUtil.builder().dataSource(dataSource).build();
        List<Map<String, Object>> rows = tdUtil.executeQuery("show dnodes");
        for (Map<String, Object> row : rows) {
            log.info("{}", row);
        }

        tdUtil.close();
    }

    @Test
    void t005() throws Exception {
        //数据源
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
        config.setJdbcUrl("jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true");
        config.setUsername("root");
        config.setPassword("taosdata");
        DataSource dataSource = new HikariDataSource(config);

        //初始化，应用全局只需要初始化一个即可
        TDengineUtil tdUtil = TDengineUtil.builder().build(dataSource);
        List<Map<String, Object>> rows = tdUtil.executeQuery("show dnodes");
        for (Map<String, Object> row : rows) {
            log.info("{}", row);
        }

        tdUtil.close();
    }


    @Test
    void t006() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
        config.setJdbcUrl("jdbc:TAOS-RS://172.16.1.173:16041/?batchfetch=true");
        config.setUsername("root");
        config.setPassword("taosdata");
        DataSource dataSource = new HikariDataSource(config);

        //初始化，应用全局只需要初始化一个即可
        TDengineUtil tdUtil = TDengineUtil.builder().build(dataSource);

        for (DateTime dateTime : DateUtil.rangeToList(DateUtil.parse("2024-08-20"), DateUtil.parse("2024-08-21"), DateField.DAY_OF_YEAR)) {
            //log.info("{} {}", dateTime.toString("yyyy-MM-dd"), DateUtil.endOfMonth(dateTime).toString("yyyy-MM-dd"));
            String sql = "select to_char(`3014`,'yyyyMMddHH24miss') time,`protocol` from frequent.d_p where did = 'NJ4GNBZAX0000172' and `3014` between '" + dateTime.toString("yyyy-MM-dd") + "' and '" + DateUtil.endOfDay(dateTime).toString("yyyy-MM-dd HH:mm:ss") + "'";
            List<Map<String, Object>> rows = tdUtil.executeQuery(sql);
            rows.forEach(row -> {
                if (!row.get("protocol").toString().contains(row.get("time").toString())) {
                    //log.error("{} {}", row.get("time"), row.get("protocol"));
                    System.out.println(row.get("protocol"));
                }
            });
        }
    }

    @Test
    void t007() {
        for (DateTime dateTime : DateUtil.rangeToList(DateUtil.parse("2024-01-01"), DateUtil.parse("2024-08-22"), DateField.MONTH)) {
            log.info("{} {}", dateTime.toString("yyyy-MM-dd"), DateUtil.endOfMonth(dateTime).toString("yyyy-MM-dd"));
        }
    }


    @Test
    void t008() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
        config.setJdbcUrl("jdbc:TAOS-RS://172.16.1.173:16041/?batchfetch=true");
        config.setUsername("root");
        config.setPassword("taosdata");
        DataSource dataSource = new HikariDataSource(config);

        //初始化，应用全局只需要初始化一个即可
        TDengineUtil tdUtil = TDengineUtil.builder().build(dataSource);

        for (String row : ResourceUtil.readUtf8Str("protocol.txt").split("\\n")) {
            long t = DateUtil.parse(row.split("3014:")[1].substring(0, 14), DatePattern.PURE_DATETIME_PATTERN).getTime() * 1000;

            tdUtil.insertRow("frequent", "d_p", "test_d_p", new TreeMap<String, Object>() {{
                put("3014", t);
                put("protocol", row);
            }});
        }

        tdUtil.awaitExecution();
        tdUtil.close();
    }


    @Test
    void 刷新v_c表缓存() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
        config.setJdbcUrl("jdbc:TAOS-RS://172.16.1.173:16041/?batchfetch=true");
        config.setUsername("root");
        config.setPassword("taosdata");
        DataSource dataSource = new HikariDataSource(config);
        TDengineUtil tdUtil = TDengineUtil.builder().build(dataSource);
        List<Map<String, Object>> rows = tdUtil.executeQuery("select table_name from information_schema.ins_tables where db_name='frequent' and table_name like 'v_c_%'", null, null);
        for (int i = 0; i < rows.size(); i++) {
            log.info("{}/{}", (i + 1), rows.size());
            Map<String, Object> m = rows.get(i);
            tdUtil.executeUpdate(StrUtil.format("select last(`4163`),last(`4592`),vin,last(`did`) from frequent.`{}`", m.get("table_name")), null, null);
        }
        tdUtil.close();
    }


    @Test
    void 修复d_p表时区数据错误() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
        config.setJdbcUrl("jdbc:TAOS-RS://172.16.1.173:16041/?batchfetch=true");
        config.setUsername("root");
        config.setPassword("taosdata");
        DataSource dataSource = new HikariDataSource(config);
        TDengineUtil tdUtil = TDengineUtil.builder().build(dataSource);

        // 从2024-08-21开始补数据，往前补
        DateTime day = DateUtil.parse("2024-08-21");
        for (DateTime dateTime : DateUtil.range(DateUtil.beginOfDay(day), DateUtil.endOfDay(day), DateField.HOUR_OF_DAY)) {
            //log.info("{} {}", dateTime.toString(), dateTime.offsetNew(DateField.HOUR, 1).toString());
            String sql = StrUtil.format("select `3014`,protocol,tbname from frequent.d_p where `3014` >= '{}' and `3014` < '{}' and protocol nmatch '.*3014:{}.*'"
                    , dateTime.toString(), dateTime.offsetNew(DateField.HOUR, 1).toString(), dateTime.toString("yyyyMMddHH"));
            log.debug("{}", sql);
            for (Map<String, Object> row : tdUtil.executeQuery(sql, null, null)) {
                String protocol = row.get("protocol").toString();
                String tbname = row.get("tbname").toString();
                String p3014 = ReUtil.getGroup1(".*,3014:(\\d{14}).*", protocol);
                if (StrUtil.isNotBlank(p3014)) {
                    String delSql = StrUtil.format("delete from frequent.`{}` where _rowts = '{}'", tbname, DateUtil.parse(row.get("3014").toString()).toString("yyyy-MM-dd HH:mm:ss"));
                    log.info("应该写入 {} 实际上写入了 {} 所以这条要删掉", p3014, row.get("3014"));
                    log.warn("{}", delSql);
                    tdUtil.executeUpdate(delSql, null, null);
                }
            }
        }

        tdUtil.close();
    }

    @Test
    void 获得3014() {
        String s = "SUBMIT$7225659615638614016$NJ4GNBZAX0000172$REALTIME$TIME:20240820185900,gw:farm,3004:18,4042:1,4030:2,4031:1,4032:12321.19,4033:2,4040:0,4041:0,4043:0,4044:1,2204:0,2603:26.1885360,2602:113.3193780,3012:0,3013:262,3014:20240820185853,3015:7,3016:9,4014:1,2601:0,3019:0";
        log.info("{}", ReUtil.getGroup1(".*,3014:(\\d{14}).*", s));
    }


}
