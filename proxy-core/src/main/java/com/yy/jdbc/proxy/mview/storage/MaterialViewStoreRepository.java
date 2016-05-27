package com.yy.jdbc.proxy.mview.storage;

import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.mview.common.BaseDAO;
import com.yy.jdbc.proxy.mview.common.storage.IMaterialViewStoreRepository;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.*;
import java.util.*;
import java.util.Date;

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
    public boolean build(Statement statement, MaterialView view) throws SQLException {
        long start = new Date().getTime();
        logger.info("物化视图数据存储开始................................");

        String tableName = view.getViewName();

        // 删除表
        logger.info("物化视图数据存储删除[" + tableName + "]开始.............");
        dropTable(statement.getConnection(), view.getViewName());
        logger.info("物化视图数据存储删除[" + tableName + "]结束.............");
        logger.info("物化视图数据存储获取查询结果[" + tableName + "]结束.............");
        logger.info("物化视图数据存储插入数据[" + tableName + "]开始.............");
        createTableBySelect(statement.getConnection(), tableName, view.getSqlStr());
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
     * {@inheritDoc}
     */
    @Override
    public boolean refresh(Statement statement, MaterialView view)
            throws SQLException {
        logger.info("物化视图数据存储刷新[" + view.getViewName() + "]开始............");
        boolean result = build(statement, view);
        logger.info("物化视图数据存储刷新[" + view.getViewName() + "]结束............");
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean drop(Statement statement, MaterialView view)
            throws SQLException {
        logger.info("物化视图数据存储删除[" + view.getViewName() + "]开始............");
        boolean result = dropTable(statement.getConnection(), view.getViewName());
        logger.info("物化视图数据存储删除[" + view.getViewName() + "]结束............");
        return result;
    }
}