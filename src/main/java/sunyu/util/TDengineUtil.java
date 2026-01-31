package sunyu.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.thread.ThreadUtil;
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
        log.info("[构建 {}] 开始", this.getClass().getSimpleName());

        log.info("[构建 {}] 结束", this.getClass().getSimpleName());

        this.config = config;
    }

    private static class Config {
        private DataSource dataSource;
        private final String insertSqlPre = "INSERT INTO";
        private final StringBuilder sqlBuilder = new StringBuilder();
        private Integer maxSqlLength = 1024 * 1024;
        private Boolean showSql = false;
    }

    public static class Builder {
        private final Config config = new Config();

        public TDengineUtil build() {
            return new TDengineUtil(config);
        }

        /**
         * 设置数据源
         *
         * @param dataSource 数据源
         * @return 构建器
         */
        public Builder dataSource(DataSource dataSource) {
            config.dataSource = dataSource;
            return this;
        }

        /**
         * 设置最大SQL长度
         * <p>
         * 默认1024 * 1024字节
         *
         * @param maxSqlLength 最大SQL长度
         * @return 构建器
         */
        public Builder setMaxSqlLength(int maxSqlLength) {
            config.maxSqlLength = maxSqlLength;
            return this;
        }

        /**
         * 是否显示执行SQL
         * <p>
         * 默认false
         *
         * @param showSql 是否显示执行SQL
         * @return 构建器
         */
        public Builder setShowSql(boolean showSql) {
            config.showSql = showSql;
            return this;
        }
    }

    /**
     * 回收资源
     */
    @Override
    public void close() {
        log.info("[销毁 {}] 开始", this.getClass().getSimpleName());
        await();
        log.info("[销毁 {}] 结束", this.getClass().getSimpleName());
    }

    synchronized private void appendSql(String sql) {
        if (sql == null) {//代表有人执行了await
            if (config.sqlBuilder.length() > 0) {
                executeInsertBatch();
            }
            return;
        }
        if (config.insertSqlPre.length() + config.sqlBuilder.length() + sql.length() >= config.maxSqlLength) {
            executeInsertBatch();
        }
        config.sqlBuilder.append(sql);
    }

    private void executeInsertBatch() {
        while (true) {//确保一定执行成功
            try {
                executeSql(config.insertSqlPre + config.sqlBuilder);
                break;
            } catch (Exception e) {
                ThreadUtil.sleep(1000 * 10); // 10秒后重试
            }
        }
        config.sqlBuilder.setLength(0);
    }

    /**
     * 等待所有任务完成
     */
    public void await() {
        appendSql(null);
    }

    /**
     * 异步插入一条记录，需要在合适的位置调用await方法，避免还未写入完毕就结束程序
     * （TDengine3.3版本开始使用这种写法）
     *
     * @param databaseName   数据库名称
     * @param superTableName 超级表名称
     * @param tableName      表名
     * @param fieldsAndTags  行数据，包括列和标签数据(key：列名或者标签名，value：列值或者标签值)
     */
    public void appendInsert(String databaseName, String superTableName, String tableName, Map<String, ?> fieldsAndTags) {
        String sql = genSqlv33(databaseName, superTableName, tableName, fieldsAndTags);
        appendSql(sql);
    }

    private String genSqlv33(String databaseName, String superTableName, String tableName, Map<String, ?> fieldsAndTags) {
        // databaseName : 数据库名
        // superTableName : 超级表名
        // tableName : 表名
        // tbname : 固定占位符，这个名称不能改变
        // fieldName : 列名
        // fieldValue : 列值
        // tagName : 标签名
        // tagValue : 标签值

        // TDengine3.3版本开始使用这种写法
        // INSERT INTO `databaseName`.`superTableName` (`tbname`,`fieldName1`,`tagName1` ,...) values ('tableName','fieldValue1','tagValue1' ,...)
        List<String> fieldAndTagNames = new ArrayList<>();
        List<String> fieldAndTagValues = new ArrayList<>();
        fieldsAndTags.forEach((key, value) -> {
            if (value != null) {
                fieldAndTagNames.add("`" + key + "`");
                fieldAndTagValues.add("'" + Convert.toStr(value) + "'");
            }
        });
        // 注意这里没有 INSERT INTO
        String sql = StrUtil.format(" `{}`.`{}` (`tbname`,{}) values ('{}',{}) ", databaseName, superTableName,
                CollUtil.join(fieldAndTagNames, ","), tableName, CollUtil.join(fieldAndTagValues, ","));
        return sql;
    }

    /**
     * 异步插入一条记录，需要在合适的位置调用await方法，避免还未写入完毕就结束程序
     * （TDengine3.3版本以前使用这种写法）
     *
     * @param databaseName   数据库名称
     * @param superTableName 超级表名称
     * @param tableName      表名
     * @param fields         列信息(key:列名称，value：列值)
     * @param tags           标签信息(key：标签名称，value：标签值)
     */
    public void appendInsert(String databaseName, String superTableName, String tableName, Map<String, ?> fields,
                             Map<String, ?> tags) {
        String sql = genSql(databaseName, superTableName, tableName, fields, tags);
        appendSql(sql);
    }

    private String genSql(String databaseName, String superTableName, String tableName, Map<String, ?> fields, Map<String, ?> tags) {
        // databaseName : 数据库名
        // superTableName : 超级表名
        // tableName : 表名
        // fieldName : 列名
        // fieldValue : 列值
        // tagName : 标签名
        // tagValue : 标签值

        // TDengine3.3版本以前使用这种写法
        // INSERT INTO `databaseName`.`tableName` USING `databaseName`.`superTableName` (`tagName1`,...) TAGS ('tagValue1',...) (`fieldName1`,...) VALUES ('fieldValue1',...)
        List<String> fieldNames = new ArrayList<>();
        List<String> fieldValues = new ArrayList<>();
        fields.forEach((key, value) -> {
            if (value != null) {
                fieldNames.add("`" + key + "`");
                fieldValues.add("'" + Convert.toStr(value) + "'");
            }
        });
        List<String> tagNames = new ArrayList<>();
        List<String> tagValues = new ArrayList<>();
        tags.forEach((key, value) -> {
            if (value != null) {
                tagNames.add("`" + key + "`");
                tagValues.add("'" + Convert.toStr(value) + "'");
            }
        });
        // 注意这里没有 INSERT INTO
        String sql = StrUtil.format(" `{}`.`{}` USING `{}`.`{}` ({}) TAGS ({}) ({}) VALUES ({}) ",
                databaseName, tableName, databaseName, superTableName, CollUtil.join(tagNames, ","),
                CollUtil.join(tagValues, ","), CollUtil.join(fieldNames, ","), CollUtil.join(fieldValues, ","));
        return sql;
    }

    /**
     * 插入一条记录 （TDengine3.3版本开始使用这种写法）
     *
     * @param databaseName   数据库名称
     * @param superTableName 超级表名称
     * @param tableName      表名
     * @param fieldsAndTags  行数据，包括列和标签数据(key：列名或者标签名，value：列值或者标签值)
     */
    public void insert(String databaseName, String superTableName, String tableName, Map<String, ?> fieldsAndTags) {
        String sql = genSqlv33(databaseName, superTableName, tableName, fieldsAndTags);
        executeSql(config.insertSqlPre + sql);
    }

    /**
     * 插入一条记录 （TDengine3.3版本以前使用这种写法）
     *
     * @param databaseName   数据库名称
     * @param superTableName 超级表名称
     * @param tableName      表名
     * @param fields         列信息(key:列名称，value：列值)
     * @param tags           标签信息(key：标签名称，value：标签值)
     */
    public void insert(String databaseName, String superTableName, String tableName, Map<String, ?> fields, Map<String, ?> tags) {
        String sql = genSql(databaseName, superTableName, tableName, fields, tags);
        executeSql(config.insertSqlPre + sql);
    }

    /**
     * 执行更新sql语句
     *
     * @param sql sql语句
     */
    public void executeSql(String sql) {
        if (config.showSql) {
            log.info("执行SQL: {}", sql);
        }
        try (Connection conn = config.dataSource.getConnection(); Statement stmt = conn.createStatement();) {
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            log.error("执行sql语句出错: {} {}", e.getMessage(), sql);
            throw new RuntimeException(e);
        }
    }

    /**
     * 查询sql语句
     *
     * @param sql 查询sql
     * @return
     */
    public List<Map<String, Object>> querySql(String sql) {
        if (config.showSql) {
            log.info("执行SQL: {}", sql);
        }
        try (Connection conn = config.dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet resultSet = stmt.executeQuery(sql);) {
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
                        case Types.NCLOB:
                            row.put(columnName, resultSet.getNClob(columnIndex));
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
                        case Types.REF_CURSOR:
                        case Types.TIME_WITH_TIMEZONE:
                        case Types.TIMESTAMP_WITH_TIMEZONE:
                        default:
                            row.put(columnName, resultSet.getObject(columnIndex));
                            break;
                    }
                }
                rows.add(row);
            }
            return rows;
        } catch (Exception e) {
            log.error("查询sql语句出错: {} {}", e.getMessage(), sql);
            throw new RuntimeException(e);
        }
    }

}