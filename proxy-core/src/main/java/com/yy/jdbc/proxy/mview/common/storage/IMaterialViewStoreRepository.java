package com.yy.jdbc.proxy.mview.common.storage;

import com.yy.jdbc.proxy.mview.MaterialView;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 物化视图数据存储服务
 *
 * @author ZhangXueJun
 */
public interface IMaterialViewStoreRepository {

    /**
     * 存储物化视图数据
     *
     * @param view
     * @return
     */
    boolean build(Connection conn, MaterialView view) throws SQLException;

    /**
     * 刷新物化视图数据
     *
     * @param view
     * @return
     */
    boolean refresh(Connection conn, MaterialView view) throws SQLException;

    /**
     * 删除物化视图数据
     *
     * @param view
     * @return
     */
    boolean drop(Connection conn, MaterialView view) throws SQLException;

}
