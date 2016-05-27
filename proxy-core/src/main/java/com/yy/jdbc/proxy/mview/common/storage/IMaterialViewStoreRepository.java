package com.yy.jdbc.proxy.mview.common.storage;

import com.yy.jdbc.proxy.mview.MaterialView;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * 物化视图数据存储服务
 *
 * @author ZhangXueJun
 */
public interface IMaterialViewStoreRepository {

    /**
     * 存储物化视图数据
     *
     *
     * @param statement
     * @param view
     * @return
     */
    boolean build(Statement statement, MaterialView view) throws SQLException;

    /**
     * 刷新物化视图数据
     *
     *
     * @param statement
     * @param view
     * @return
     */
    boolean refresh(Statement statement, MaterialView view) throws SQLException;

    /**
     * 删除物化视图数据
     *
     *
     * @param statement
     * @param view
     * @return
     */
    boolean drop(Statement statement, MaterialView view) throws SQLException;

}
