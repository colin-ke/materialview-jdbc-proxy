package com.yy.jdbc.proxy.mview;

import com.yy.jdbc.proxy.mview.common.manager.IMaterialViewProvider;
import com.yy.jdbc.proxy.mview.manager.MaterialViewProvider;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
            Statement stmt) throws SQLException {
        return provider.create(stmt, view);
    }

    /**
     * 删除物化视图
     */
    public static Boolean dropMaterialView(
            String viewName,
            Statement stmt) throws SQLException {
        return provider.delete(stmt, viewName);
    }

    /**
     * 刷新物化视图
     */
    public static Boolean refreshMaterialView(
            String viewName,
            Statement stmt) throws SQLException {
        return provider.refresh(stmt, viewName);
    }


    /**
     * 通过物理事实表查找出所有能被用来rewrite的物化视图
     */
    public static Collection<MaterialView> getMaterialViewListByFactTable(
            String factTable,
            Statement stmt) throws SQLException {
        return provider.getMaterialViewListByFactTable(stmt, factTable);
    }

    /**
     * 查找所有视图, 用于第三方使用
     * <p/>
     * 类似命令： show tables
     */
    public static boolean showMaterialViews(
    		Statement stmt) throws SQLException {
        return provider.showMaterialViews(stmt);
    }

    /**
     * 通过物化视图名称查找出所有能被用来rewrite的物化视图原始创建语句， , 用于第三方使用
     * <p/>
     * 类似命令：show create table tableName
     */
    public static boolean showCreateMaterialView(
            Statement stmt, String viewName) throws SQLException {
        return provider.showCreateMaterialView(stmt, viewName);
    }
}