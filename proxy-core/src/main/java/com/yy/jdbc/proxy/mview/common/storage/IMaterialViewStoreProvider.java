package com.yy.jdbc.proxy.mview.common.storage;

import com.yy.jdbc.proxy.mview.MaterialView;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 物化视图数据存储服务接口
 *
 * @author ZhangXueJun
 */
public interface IMaterialViewStoreProvider {

    /**
     * 物化视图数据存储
     *
     * @param view
     * @return
     */
    boolean build(Connection conn, MaterialView view)
            throws SQLException;

    /**
     * 物化视图数据刷新
     *
     * @param view
     * @return
     */
    boolean refresh(Connection conn, MaterialView view) throws SQLException;

    /**
     * 物化视图数据删除
     *
     * @param view
     * @return
     */
    boolean drop(Connection conn, MaterialView view) throws SQLException;
}
