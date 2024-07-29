package sunyu.util;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.thread.BlockPolicy;
import cn.hutool.core.thread.ExecutorBuilder;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.Serializable;
import java.sql.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TDengine工具类
 * <p>
 * 主要用于缓冲写入sql，到达批量写入效果，提升写入效率，同时也拥有查询功能
 *
 * @author 孙宇
 */
public enum TDengineUtil implements Serializable, Closeable {
    INSTANCE;
    private Log log = LogFactory.get();
    private StringBuilder sqlCache = new StringBuilder();
    private DataSource dataSource;
    private ThreadPoolExecutor threadPoolExecutor;
    private int maxSqlLength = 1024 * 512;
    private int maxPoolSize = 1;
    private int maxWorkQueue = 1;
    private String insertPre = "INSERT INTO";
    private ReentrantLock lock = new ReentrantLock();


    /**
     * 设置数据源
     *
     * @param dataSource
     * @return
     */
    public TDengineUtil setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return INSTANCE;
    }

    /**
     * 设置最大线程数
     *
     * @param maxPoolSize
     * @return
     */
    public TDengineUtil setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        return INSTANCE;
    }

    /**
     * 构建工具类
     *
     * @return
     */
    public TDengineUtil build() {
        if (dataSource == null) {
            threadPoolExecutor = ExecutorBuilder.create()
                    .setCorePoolSize(0)
                    .setMaxPoolSize(maxPoolSize)
                    .setWorkQueue(new LinkedBlockingQueue<>(maxWorkQueue))
                    .setHandler(new BlockPolicy(runnable -> {
                        log.error("向线程的阻塞队列put数据出现了异常");
                    }))
                    .build();
            log.debug("数据源:{}", dataSource);
            log.debug("最大线程数量 {}", maxPoolSize);
            log.debug("线程最大工作队列 {}", maxWorkQueue);
            log.debug("TDengineUtil初始化完毕");
        }
        return INSTANCE;
    }

    /**
     * ResultSet回调
     */
    public interface ResultSetCallback {
        /**
         * 执行回调
         *
         * @param resultSet 结果集
         */
        void exec(ResultSet resultSet) throws SQLException;
    }


    /**
     * 写入一行数据
     * <p>
     * 会先写入sql缓存，如果sql缓存超出大小，则会放入执行队列，由多线程执行sql
     *
     * @param databaseName 数据库名称
     * @param superTable   超级表名称
     * @param tableName    表名称
     * @param row          一行数据内容
     */
    public void insertRow(String databaseName, String superTable, String tableName, TreeMap<String, Object> row) {
        try {
            lock.lock();
            String subSql = getSubSql(databaseName, superTable, tableName, row);
            if (insertPre.length() + sqlCache.length() + subSql.length() > maxSqlLength) {
                String sql = insertPre + sqlCache.toString();
                sqlCache.setLength(0);
                threadPoolExecutor.execute(() -> {
                    executeUpdate(sql, null, null);
                });
            }
            sqlCache.append(subSql);
        } finally {
            lock.unlock();
        }
    }


    /**
     * 写入多行数据
     * <p>
     * 多行数据会写入到同一个表中，会先写入sql缓存，如果sql缓存超出大小，则会放入执行队列，由多线程执行sql
     *
     * @param databaseName 数据库名称
     * @param superTable   超级表名称
     * @param tableName    表名
     * @param rows         多行数据内容
     */
    public void insertRows(String databaseName, String superTable, String tableName, List<TreeMap<String, Object>> rows) {
        for (TreeMap<String, Object> row : rows) {
            insertRow(databaseName, superTable, tableName, row);
        }
    }

    private String getSubSql(String databaseName, String superTable, String tableName, TreeMap<String, Object> row) {
        //最终拼装后的格式：
        // `databaseName`.`superTableName` (`tbname`,`column1`,`tag1`) values ('表名','列值','标签值')
        StringBuilder subSqlCache = new StringBuilder();
        // `databaseName`.`superTableName` (`tbname`
        subSqlCache.append(StrUtil.format(" `{}`.`{}` (`tbname`", databaseName, superTable));
        //,`column1`,`tag1`
        row.forEach((key, value) -> {
            subSqlCache.append(StrUtil.format(",`{}`", key));
        });
        //) values
        subSqlCache.append(") values ");
        //('表名'
        subSqlCache.append(StrUtil.format("('{}'", tableName));
        //,'列值','标签值'
        row.forEach((key, value) -> {
            if (value != null) {
                subSqlCache.append(StrUtil.format(",'{}'", Convert.toStr(value)));
            } else {
                subSqlCache.append(",null");
            }
        });
        //)
        subSqlCache.append(")");
        return subSqlCache.toString();
    }

    /**
     * 执行sql语句
     *
     * @param sql         sql语句
     * @param retry       重试次数，如果为null则无限重试
     * @param sleepMillis 重试睡眠间隔，单位毫秒，如果为null，则间隔时间为1000*5毫秒
     * @return 响应数量
     */
    public int executeUpdate(String sql, Integer retry, Integer sleepMillis) {
        //log.debug("Executing SQL: {}", sql);
        int i = -1;
        while (retry == null || retry-- > 0) {
            try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement();) {
                i = stmt.executeUpdate(sql);
                break;
            } catch (Exception e) {
                log.error("执行sql语句出错: {} {}", e.getMessage(), sql);
                if (sleepMillis != null) {
                    ThreadUtil.safeSleep(sleepMillis);
                } else {
                    ThreadUtil.safeSleep(1000 * 5);
                }
            }
        }
        return i;
    }

    /**
     * 查询sql语句
     *
     * @param sql      sql语句
     * @param callback 回调函数
     */
    public void executeQuery(String sql, ResultSetCallback callback) {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement(); ResultSet resultSet = stmt.executeQuery(sql);) {
            callback.exec(resultSet);
        } catch (Exception e) {
            log.error("查询sql语句出错: {} {}", e.getMessage(), sql);
        }
    }

    /**
     * 查询sql语句
     *
     * @param sql sql语句
     * @return 查询结果
     */
    public List<Map<String, Object>> executeQuery(String sql) {
        List<Map<String, Object>> rows = new ArrayList<>();
        executeQuery(sql, resultSet -> {
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
        });
        return rows;
    }

    /**
     * 等待sql缓存执行完毕
     */
    public void awaitExecution() {
        try {
            lock.lock();
            if (sqlCache.length() > 0) {
                String sql = insertPre + sqlCache.toString();
                sqlCache.setLength(0);
                threadPoolExecutor.execute(() -> {
                    executeUpdate(sql, null, null);
                });
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 回收资源，等待sql缓存和所有线程队列执行完毕
     */
    @Override
    public void close() {
        log.debug("准备回收资源");
        awaitExecution();
        threadPoolExecutor.shutdown();
        try {
            threadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            log.error("回收资源出现中断异常 {}", e.getMessage());
        } catch (Exception e) {
            log.error("回收资源出现异常 {}", e.getMessage());
        }
        log.debug("资源回收完毕");
    }
}