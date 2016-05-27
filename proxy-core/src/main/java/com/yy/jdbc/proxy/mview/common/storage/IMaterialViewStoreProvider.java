package com.yy.jdbc.proxy.mview.common.storage;

import com.yy.jdbc.proxy.mview.MaterialView;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * 物化视图数据存储服务接口
 *
 * @author ZhangXueJun
 */
public interface IMaterialViewStoreProvider {

    /**
     * 物化视图数据存储
     *
     *
     * @param statement
     * @param view
     * @return
     */
    boolean build(Statement statement, MaterialView view)
            throws SQLException;

    /**
     * 物化视图数据刷新
     *
     *
     * @param statement
     * @param view
     * @return
     */
    boolean refresh(Statement statement, MaterialView view) throws SQLException;

    /**
     * 物化视图数据删除
     *
     *
     * @param statement
     * @param view
     * @return
     */
    boolean drop(Statement statement, MaterialView view) throws SQLException;
}
