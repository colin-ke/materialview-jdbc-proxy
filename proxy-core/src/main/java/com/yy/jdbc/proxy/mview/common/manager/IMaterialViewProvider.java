package com.yy.jdbc.proxy.mview.common.manager;

import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.mview.common.Provider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

/**
 * 物化视图服务接口
 *
 * @author ZhangXueJun
 */
public interface IMaterialViewProvider extends Provider<MaterialView> {

    /**
     * 根据事实表获取相关的物化视图
     *
     * @param conn
     * @param factTable
     * @return
     */
    Collection<MaterialView> getMaterialViewListByFactTable(
            Connection conn, String factTable) throws SQLException;

    /**
     * 刷新物化视图
     *
     * @param conn
     * @param viewName
     * @return
     */
    boolean refresh(Connection conn, String viewName) throws SQLException;

    /**
     * 查找所有视图, 用于第三方使用
     * <p/>
     * 类似命令： show tables
     *
     * @param conn
     * @return
     */
    ResultSet showMaterialViews(Connection conn) throws SQLException;

    /**
     * 通过物化视图名称查找出所有能被用来rewrite的物化视图原始创建语句， , 用于第三方使用
     * <p/>
     * 类似命令：show create table tableName
     *
     * @param conn
     * @param viewName
     * @return
     */
    ResultSet showCreateMaterialView(Connection conn, String viewName) throws SQLException;
}