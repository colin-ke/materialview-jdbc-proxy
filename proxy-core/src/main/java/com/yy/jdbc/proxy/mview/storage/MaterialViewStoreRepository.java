package com.yy.jdbc.proxy.mview.storage;

import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.mview.common.BaseDAO;
import com.yy.jdbc.proxy.mview.common.storage.IMaterialViewStoreRepository;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * 物化视图数据存储持久层
 *
 * @author ZhangXueJun
 */
public class MaterialViewStoreRepository
        extends BaseDAO
        implements IMaterialViewStoreRepository {

    // 批量执行的记录的数量的阀值
    public final static int threshold = 1000;

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean build(Connection conn, MaterialView view) throws SQLException {
        long start = new Date().getTime();
        logger.info("物化视图数据存储开始................................");

        String tableName = view.getViewName();

        // 删除表
        logger.info("物化视图数据存储删除[" + tableName + "]开始.............");
        dropTable(conn, view.getViewName());
        logger.info("物化视图数据存储删除[" + tableName + "]结束.............");
        logger.info("物化视图数据存储获取查询结果[" + tableName + "]结束.............");
        logger.info("物化视图数据存储插入数据[" + tableName + "]开始.............");
        createTableBySelect(conn, tableName, view.getSqlStr());
        logger.info("物化视图数据存储插入数据[" + tableName + "]结束.............");
        long end = new Date().getTime();
        logger.info("物化视图数据存储结束，总耗时: " + (end - start) / 1000 + "秒.");
        return true;
    }

    private boolean createTableBySelect(Connection conn, String tableName, String sqlStr) throws SQLException {
        StringBuilder builder = new StringBuilder();
        builder.append(" CREATE TABLE " + tableName + " AS " + sqlStr);
        runner.insert(conn, builder.toString(), new ResultSetHandler<Object>() {
            @Override
            public Object handle(ResultSet rs) throws SQLException {
                return null;
            }
        });
        return true;
    }

    private boolean selectInsert(
            Connection conn,
            String tableName,
            String sqlStr,
            Collection<String> columns) throws SQLException {
        StringBuilder builder = new StringBuilder();
        builder.append("insert into " + tableName + "(");

        String fields = "";
        for (String column : columns) {
            fields += column + ",";
        }

        builder.append(fields);
        builder.deleteCharAt(builder.length() - 1);
        builder.append(") " + sqlStr);
        runner.insert(conn, builder.toString(), new ResultSetHandler<Object>() {
            @Override
            public Object handle(ResultSet rs) throws SQLException {
                return null;
            }
        });
        return true;
    }

    /**
     * 删除表
     *
     * @param conn
     * @param tableName
     * @return
     * @throws SQLException
     */
    private boolean dropTable(Connection conn, String tableName)
            throws SQLException {
        runner.update(conn, "drop table if exists " + tableName + ";");
        return true;
    }

    /**
     * 执行SQL语句创建表
     *
     * @param conn
     * @param builder
     * @return
     * @throws SQLException
     */
    private boolean createTable(Connection conn, StringBuilder builder)
            throws SQLException {
        runner.update(conn, builder.toString());
        return true;
    }

    /**
     * 批量执行插入语句，具备事务
     *
     * @param tableName
     * @param result
     * @param columns
     * @return
     * @throws SQLException
     */
    private boolean batchInsert(
            Connection conn,
            String tableName,
            List<Object[]> result,
            Collection<String> columns) throws SQLException {
        int size = result.size();

        logger.info("物化视图数据存储批量插入数据[" + tableName + "]总条数: " + size + ".");


        // 批量执行
        try {
            conn.setAutoCommit(false);
            for (int fromIndex = 0; fromIndex < size; fromIndex += threshold) {
                int toIndex = fromIndex + threshold;
                if (toIndex > size) {
                    toIndex = size;
                }
                List<Object[]> subList = result.subList(fromIndex, toIndex);
                runner.batch(conn, insertSQL(tableName, columns), params(columns, subList));
                logger.info("物化视图数据存储批量插入数据[" + tableName + "]成功条数: " + subList.size()
                        + ", 剩余条数: " + (size - toIndex) + ".");
            }
        } finally {
            conn.commit();
            conn.setAutoCommit(true);
        }
        return true;
    }

    /**
     * 获取二维数组
     *
     * @param columns
     * @param result
     * @return
     */
    private Object[][] params(Collection<String> columns, List<Object[]> result) {
        Object[][] params = new Object[result.size()][columns.size()];
        for (int index = 0; index < result.size(); index++) {
            Object[] objects = result.get(index);

            int i = 0;
            for (String column : columns) {
                params[index][i] = objects[i];
                i++;
            }
        }
        return params;
    }

    /**
     * 批量插入语句
     *
     * @param tableName 表名
     * @param columns   列名集合
     * @return
     */
    private String insertSQL(String tableName, Collection<String> columns) {
        StringBuilder sql = new StringBuilder();

        sql.append("insert into " + tableName + "(");
        for (String column : columns) {
            sql.append(column + ",");
        }

        sql.deleteCharAt(sql.length() - 1);
        sql.append(") values");

        sql.append("( ");
        for (String column : columns) {
            sql.append("?,");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")").append("\n");
        return sql.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean refresh(Connection conn, MaterialView view)
            throws SQLException {
        logger.info("物化视图数据存储刷新[" + view.getViewName() + "]开始............");
        boolean result = build(conn, view);
        logger.info("物化视图数据存储刷新[" + view.getViewName() + "]结束............");
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean drop(Connection conn, MaterialView view)
            throws SQLException {
        logger.info("物化视图数据存储删除[" + view.getViewName() + "]开始............");
        boolean result = dropTable(conn, view.getViewName());
        logger.info("物化视图数据存储删除[" + view.getViewName() + "]结束............");
        return result;
    }

    /**
     * <pre>
     * 物化视图存储ArrayListHandler。相关产出:
     * <li>建表语句</li>
     * <li>列名集合</li>
     * <li>插叙结果</li>
     * </pre>
     */
    private class MaterialViewStoreHandler implements ResultSetHandler<Object> {
        /**
         * 建表语句
         */
        private StringBuilder result;

        /**
         * 物化试图名称
         */
        private String tableName;

        /**
         * 列名集合
         */
        private Collection<String> columns;

        public MaterialViewStoreHandler(
                StringBuilder result,
                Collection<String> columns,
                String tableName) {
            super();
            this.result = result;
            this.tableName = tableName;
            this.columns = columns;
        }

        @Override
        public Object handle(ResultSet rs) throws SQLException {
            Collection<String> keys = new LinkedList<String>();

            // 创建表语句
            result.append("CREATE TABLE " + tableName + " (").append("\n");

            ResultSetMetaData metaData = rs.getMetaData();
            int cols = metaData.getColumnCount();
            for (int i = 1; i <= cols; i++) {
                String name = metaData.getColumnName(i);
                String typeName = metaData.getColumnTypeName(i);

                // 字段长度
                int size = metaData.getPrecision(i);

                // 精度
                int scale = metaData.getScale(i);

                Class clazz = null;
                try {
                    clazz = Class.forName(metaData.getColumnClassName(i));
                    columns.add(name);
                } catch (ClassNotFoundException e) {
                    throw new SQLException(e);
                }

                // 不需要长度的字段类型
                Collection<Class> notNeedLengthList = Arrays.asList(
                        new Class[]
                                {
                                        java.sql.Date.class,
                                        java.sql.Time.class,
                                        java.sql.Timestamp.class,
                                        byte[].class
                                }
                );

                int tmp = 0;
                if (clazz == String.class && size > 4000) {
                    typeName = "text";
                    tmp = -1;
                } else if (notNeedLengthList.contains(clazz)) {
                    tmp = -1;
                }

                if (scale != 0) {
                    if (scale > size) {
                        scale = size;
                    }
                    result.append(" " + name + " " + typeName + (tmp == 0 ? "(" + size + "," + scale + ") ," : ",")).append("\n");
                } else {
                    result.append(" " + name + " " + typeName + (tmp == 0 ? "(" + size + ") ," : ",")).append("\n");
                }

                if (size <= 256 && !notNeedLengthList.contains(clazz)) { // 索引列过长无太大意义
                    keys.add(metaData.getColumnName(i));
                }
            }

            // 创建索引
            for (String key : keys) {
                result.append(" KEY `" + key + "_index" + "` (");
                result.append(key + "),").append("\n");
            }

            result.deleteCharAt(result.length() - 1);
            result.deleteCharAt(result.length() - 1);
            result.append("\n");
            result.append(") ENGINE=MyISAM DEFAULT CHARSET=utf8;");

            logger.info("物化视图数据存储[" + tableName + "]建表语句：\n" + result);
            return null;
        }
    }
}