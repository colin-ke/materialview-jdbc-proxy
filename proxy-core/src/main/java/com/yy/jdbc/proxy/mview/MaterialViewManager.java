package com.yy.jdbc.proxy.mview;

import com.yy.jdbc.proxy.mview.common.manager.IMaterialViewProvider;
import com.yy.jdbc.proxy.mview.manager.MaterialViewProvider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

/**
 * 物化视图管理【提供给外部静态方法】
 *
 * @author ZhangXueJun
 */
public class MaterialViewManager {

    private static IMaterialViewProvider provider = new MaterialViewProvider();

    /**
     * 创建物化视图
     */
    public static Boolean createMaterialView(
            MaterialView view,
            Connection conn) throws SQLException {
        return provider.create(conn, view);
    }

    /**
     * 删除物化视图
     */
    public static Boolean dropMaterialView(
            String viewName,
            Connection conn) throws SQLException {
        return provider.delete(conn, viewName);
    }

    /**
     * 刷新物化视图
     */
    public static Boolean refreshMaterialView(
            String viewName,
            Connection conn) throws SQLException {
        return provider.refresh(conn, viewName);
    }


    /**
     * 通过物理事实表查找出所有能被用来rewrite的物化视图
     */
    public static Collection<MaterialView> getMaterialViewListByFactTable(
            String factTable,
            Connection conn) throws SQLException {
        return provider.getMaterialViewListByFactTable(conn, factTable);
    }

    /**
     * 查找所有视图, 用于第三方使用
     * <p/>
     * 类似命令： show tables
     */
    public static ResultSet showMaterialViews(
            Connection conn) throws SQLException {
        return provider.showMaterialViews(conn);
    }

    /**
     * 通过物化视图名称查找出所有能被用来rewrite的物化视图原始创建语句， , 用于第三方使用
     * <p/>
     * 类似命令：show create table tableName
     */
    public static ResultSet showCreateMaterialView(
            Connection conn, String viewName) throws SQLException {
        return provider.showCreateMaterialView(conn, viewName);
    }
}