package sunyu.util.test;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.thread.ThreadUtil;
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
        TDengineUtil tdUtil = TDengineUtil.INSTANCE.setDataSource(dataSource).setMaxPoolSize(5).build();

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
        TDengineUtil tdUtil = TDengineUtil.INSTANCE.setDataSource(dataSource).setMaxPoolSize(5).build();

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
    void t003() {
        //数据源
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.taosdata.jdbc.rs.RestfulDriver");
        config.setJdbcUrl("jdbc:TAOS-RS://192.168.13.87:16042/?batchfetch=true");
        config.setUsername("root");
        config.setPassword("taosdata");
        DataSource dataSource = new HikariDataSource(config);

        //初始化，应用全局只需要初始化一个即可
        TDengineUtil tdUtil = TDengineUtil.INSTANCE.setDataSource(dataSource).setMaxPoolSize(5).build();
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
}