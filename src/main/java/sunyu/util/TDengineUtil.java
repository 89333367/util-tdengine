package sunyu.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.exceptions.ExceptionUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TDengine工具类
 * <p>
 * 提供并发写入能力
 *
 * @author 孙宇
 */
public class TDengineUtil implements AutoCloseable {
    private final Log log = LogFactory.get();
    private final Config config;

    public static Builder builder() {
        return new Builder();
    }

    private TDengineUtil(Config config) {
        log.info("[构建TDengineUtil] 开始");
        config.asyncTaskUtil = AsyncTaskUtil.builder().setMaxConcurrency(config.maxConcurrency).build();
        log.info("[构建TDengineUtil] 结束");

        this.config = config;
    }

    private static class Config {
        private DataSource dataSource;
        private AsyncTaskUtil asyncTaskUtil;
        private Integer maxConcurrency = 10;
    }

    public static class Builder {
        private final Config config = new Config();

        public TDengineUtil build() {
            return new TDengineUtil(config);
        }

        /**
         * 设置数据源
         *
         * @param dataSource
         * @return
         */
        public Builder dataSource(DataSource dataSource) {
            config.dataSource = dataSource;
            return this;
        }

        /**
         * 设置最大并发执行数量
         *
         * @param maxConcurrency
         * @return
         */
        public Builder setMaxConcurrency(int maxConcurrency) {
            config.maxConcurrency = maxConcurrency;
            return this;
        }
    }

    /**
     * 回收资源
     */
    @Override
    public void close() {
        log.info("[销毁TDengineUtil] 开始");
        config.asyncTaskUtil.awaitAllTasks();
        config.asyncTaskUtil.close();
        log.info("[销毁TDengineUtil] 结束");
    }

    /**
     * 等待所有任务完成
     */
    public void awaitAllTasks() {
        config.asyncTaskUtil.awaitAllTasks();
    }

    /**
     * 异步插入一条记录，需要在合适的位置调用awaitAllTasks方法，避免还未写入完毕就结束程序
     *
     * @param databaseName 数据库名称
     * @param superTable   超级表名称
     * @param tableName    表名
     * @param row          行数据，包括标签数据
     */
    public void asyncInsertRow(String databaseName, String superTable, String tableName, Map<String, ?> row) {
        config.asyncTaskUtil.submitTask(() -> insertRow(databaseName, superTable, tableName, row), null, 1000 * 10);
    }


    /**
     * 插入一条记录
     *
     * @param databaseName 数据库名称
     * @param superTable   超级表名称
     * @param tableName    表名
     * @param row          行数据，包括标签数据
     */
    public void insertRow(String databaseName, String superTable, String tableName, Map<String, ?> row) {
        List<String> columnNames = new ArrayList<>();
        List<String> columnValues = new ArrayList<>();
        row.forEach((key, value) -> {
            columnNames.add("`" + key + "`");
            if (value != null) {
                columnValues.add("'" + Convert.toStr(value) + "'");
            } else {
                columnValues.add(null);
            }
        });
        // INSERT INTO `databaseName`.`superTableName` (`tbname`,`column1`,`tag1` ,...) values ('表名','列值1','标签值1' ,...)
        String sql = StrUtil.format("INSERT INTO `" + databaseName + "`.`" + superTable + "` (`tbname`,{}) values ('" + tableName + "',{})"
                , CollUtil.join(columnNames, ",")
                , CollUtil.join(columnValues, ",")
        );
        executeUpdate(sql);
    }

    /**
     * 执行更新sql语句
     *
     * @param sql sql语句
     */
    public void executeUpdate(String sql) {
        try (Connection conn = config.dataSource.getConnection(); Statement stmt = conn.createStatement();) {
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            log.error("执行sql语句出错: {} {}", ExceptionUtil.stacktraceToString(e), sql);
            throw new RuntimeException(e);
        }
    }

    /**
     * 查询sql语句
     *
     * @param sql 查询sql
     * @return
     */
    public List<Map<String, Object>> executeQuery(String sql) {
        try (Connection conn = config.dataSource.getConnection(); Statement stmt = conn.createStatement(); ResultSet resultSet = stmt.executeQuery(sql);) {
            List<Map<String, Object>> rows = new ArrayList<>();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                    int columnType = metaData.getColumnType(columnIndex);
                    String columnName = metaData.getColumnLabel(columnIndex);
                    switch (columnType) {
                        case Types.INTEGER:
                        case Types.SMALLINT:
                        case Types.TINYINT:
                            try {
                                row.put(columnName, resultSet.getInt(columnIndex));
                            } catch (SQLException e) {
                                row.put(columnName, resultSet.getLong(columnIndex));
                            }
                            break;
                        case Types.BIGINT:
                        case Types.NUMERIC:
                        case Types.DECIMAL:
                            row.put(columnName, resultSet.getBigDecimal(columnIndex));
                            break;
                        case Types.FLOAT:
                        case Types.REAL:
                            row.put(columnName, resultSet.getFloat(columnIndex));
                            break;
                        case Types.DOUBLE:
                            row.put(columnName, resultSet.getDouble(columnIndex));
                            break;
                        case Types.BOOLEAN:
                        case Types.BIT:
                            row.put(columnName, resultSet.getBoolean(columnIndex));
                            break;
                        case Types.DATE:
                            row.put(columnName, resultSet.getDate(columnIndex));
                            break;
                        case Types.TIME:
                            row.put(columnName, resultSet.getTime(columnIndex));
                            break;
                        case Types.TIMESTAMP:
                            row.put(columnName, resultSet.getTimestamp(columnIndex));
                            break;
                        case Types.CHAR:
                        case Types.VARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NCHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            row.put(columnName, resultSet.getString(columnIndex));
                            break;
                        case Types.BINARY:
                        case Types.VARBINARY:
                        case Types.LONGVARBINARY:
                            row.put(columnName, resultSet.getBytes(columnIndex));
                            break;
                        case Types.BLOB:
                            row.put(columnName, resultSet.getBlob(columnIndex));
                            break;
                        case Types.CLOB:
                            row.put(columnName, resultSet.getClob(columnIndex));
                            break;
                        case Types.ARRAY:
                            row.put(columnName, resultSet.getArray(columnIndex));
                            break;
                        case Types.REF:
                            row.put(columnName, resultSet.getRef(columnIndex));
                            break;
                        case Types.DATALINK:
                            row.put(columnName, resultSet.getURL(columnIndex));
                            break;
                        case Types.SQLXML:
                            row.put(columnName, resultSet.getSQLXML(columnIndex));
                            break;
                        case Types.ROWID:
                            row.put(columnName, resultSet.getRowId(columnIndex));
                            break;
                        case Types.NULL:
                            row.put(columnName, null);
                            break;
                        case Types.JAVA_OBJECT:
                        case Types.STRUCT:
                        default:
                            row.put(columnName, resultSet.getObject(columnIndex));
                            break;
                    }
                }
                rows.add(row);
            }
            return rows;
        } catch (SQLException e) {
            log.error("查询sql语句出错: {} {}", ExceptionUtil.stacktraceToString(e), sql);
            throw new RuntimeException(e);
        }
    }

}