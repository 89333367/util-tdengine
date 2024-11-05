package sunyu.util;

import cn.hutool.core.collection.CollUtil;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class TDengineUtil implements Serializable, Closeable {
    private final Log log = LogFactory.get();

    private final StringBuilder sqlCache = new StringBuilder();
    private DataSource dataSource;
    private ThreadPoolExecutor threadPoolExecutor;
    private int maxSqlLength = 1024 * 512;
    private int maxPoolSize = 1;
    private int maxWorkQueue = 10;
    private final String insertPre = "INSERT INTO";
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * ResultSet回调
     */
    public interface ResultSetCallback {
        /**
         * 执行回调
         *
         * @param resultSet 结果集
         */
        void exec(ResultSet resultSet) throws Exception;
    }


    /**
     * 设置数据源
     *
     * @param ds
     * @return
     */
    public TDengineUtil dataSource(DataSource ds) {
        this.dataSource = ds;
        return this;
    }

    /**
     * 设置最大线程数
     *
     * @param size
     * @return
     */
    public TDengineUtil maxPoolSize(int size) {
        this.maxPoolSize = size;
        return this;
    }


    /**
     * 设置最大阻塞队列数
     *
     * @param size
     * @return
     */
    public TDengineUtil maxWorkQueue(int size) {
        this.maxWorkQueue = size;
        return this;
    }

    /**
     * 设置一条sql最大长度
     *
     * @param length
     * @return
     */
    public TDengineUtil maxSqlLength(int length) {
        this.maxSqlLength = length;
        return this;
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
    public void insertRow(String databaseName, String superTable, String tableName, Map<String, Object> row) {
        try {
            lock.lock();
            String subSql = getSubSql(databaseName, superTable, tableName, row);
            if (insertPre.length() + sqlCache.length() + subSql.length() > maxSqlLength) {
                String sql = insertPre + sqlCache.toString();
                sqlCache.setLength(0);
                threadPoolExecutor.execute(() -> {
                    executeUpdate(sql, null, 1000 * 60);
                });
            }
            sqlCache.append(subSql);
        } finally {
            lock.unlock();
        }
    }

    private String getSubSql(String databaseName, String superTable, String tableName, Map<String, Object> row) {
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
        //最终拼装后的格式(注意前面要留个空格)：
        // `databaseName`.`superTableName` (`tbname`,`column1`,`tag1` ,...) values ('表名','列值1','标签值1' ,...)
        String subSql = StrUtil.format(" `" + databaseName + "`.`" + superTable + "` (`tbname`,{}) values ('" + tableName + "',{})"
                , CollUtil.join(columnNames, ",")
                , CollUtil.join(columnValues, ",")
        );
        //log.debug(subSql);
        return subSql;
    }

    /**
     * 执行sql语句
     *
     * @param sql         sql语句
     * @param retry       重试次数，如果为null则无限重试，0为只执行一次
     * @param sleepMillis 重试睡眠间隔，单位毫秒，如果为null，则间隔时间为1000*5毫秒
     * @return 响应数量，如果返回-1，则代表更新异常
     */
    public int executeUpdate(String sql, Integer retry, Integer sleepMillis) {
        //log.debug("Executing SQL: {}", sql);
        int i = -1;
        while (retry == null || retry >= 0) {
            try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement();) {
                i = stmt.executeUpdate(sql);
                break;
            } catch (Exception e) {
                log.error("执行sql语句出错: {} {}", e.getMessage(), sql);
                if (sleepMillis != null) {
                    ThreadUtil.sleep(sleepMillis);
                } else {
                    ThreadUtil.sleep(1000 * 5);
                }
                if (retry != null) {
                    retry--;
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
    public void executeQuery(String sql, ResultSetCallback callback) throws Exception {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement(); ResultSet resultSet = stmt.executeQuery(sql);) {
            callback.exec(resultSet);
        }
    }

    /**
     * 查询sql语句
     *
     * @param sql         sql语句
     * @param retry       重试次数，如果为null则无限重试，0为只执行一次
     * @param sleepMillis 重试睡眠间隔，单位毫秒，如果为null，则间隔时间为1000*5毫秒
     * @return 查询结果
     */
    public List<Map<String, Object>> executeQuery(String sql, Integer retry, Integer sleepMillis) {
        while (retry == null || retry >= 0) {
            try {
                return executeQuery(sql);
            } catch (Exception e) {
                log.error("查询sql语句出错: {} {}", e.getMessage(), sql);
                if (sleepMillis != null) {
                    ThreadUtil.sleep(sleepMillis);
                } else {
                    ThreadUtil.sleep(1000 * 5);
                }
                if (retry != null) {
                    retry--;
                }
            }
        }
        return null;
    }

    /**
     * 查询sql语句
     *
     * @param sql sql语句
     * @return 查询结果
     */
    public List<Map<String, Object>> executeQuery(String sql) throws Exception {
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
            waitUntilAllTasksComplete();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 等待所有任务执行完毕
     */
    private void waitUntilAllTasksComplete() {
        while (threadPoolExecutor.getActiveCount() != 0 || !threadPoolExecutor.getQueue().isEmpty()) {
            ThreadUtil.sleep(100);
        }
    }


    /**
     * 私有构造，避免外部初始化
     */
    private TDengineUtil() {
    }

    /**
     * 获得工具类工厂
     *
     * @return
     */
    public static TDengineUtil builder() {
        return new TDengineUtil();
    }


    public TDengineUtil build(DataSource dataSource) {
        log.info("构建工具类开始");

        if (threadPoolExecutor != null) {
            log.warn("工具类已构建，请不要重复构建");
            return this;
        }

        if (dataSource == null) {
            throw new RuntimeException("数据源参数异常，未正确传递数据源参数");
        }
        log.info("设置数据源开始");
        this.dataSource = dataSource;
        log.info("设置数据源结束");
        log.info("创建线程池开始");
        threadPoolExecutor = ExecutorBuilder.create()
                .setCorePoolSize(0)
                .setMaxPoolSize(maxPoolSize)
                .setWorkQueue(new LinkedBlockingQueue<>(maxWorkQueue))
                .setHandler(new BlockPolicy(runnable -> {
                    log.error("向线程的阻塞队列put数据出现了异常");
                }))
                .build();
        log.info("创建线程池完毕");
        log.info("数据源 {} {}", this.dataSource.getClass().getName(), this.dataSource.hashCode());
        log.info("最大线程数量 {}", maxPoolSize);
        log.info("线程最大工作队列 {}", maxWorkQueue);
        log.info("构建工具类完毕");
        return this;
    }

    /**
     * 构建工具类
     *
     * @return
     */
    public TDengineUtil build() {
        return build(dataSource);
    }

    /**
     * 回收资源，等待sql缓存和所有线程队列执行完毕
     */
    @Override
    public void close() {
        log.info("销毁工具类开始");
        log.info("等待sql缓存执行开始");
        awaitExecution();
        log.info("等待sql缓存执行完毕");
        log.info("关闭线程池开始");
        threadPoolExecutor.shutdown();
        try {
            threadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            log.info("关闭线程池完毕");
        } catch (InterruptedException e) {
            log.error("回收资源出现中断异常 {}", e.getMessage());
        } catch (Exception e) {
            log.error("回收资源出现异常 {}", e.getMessage());
        }
        log.info("销毁工具类完毕");
    }


}