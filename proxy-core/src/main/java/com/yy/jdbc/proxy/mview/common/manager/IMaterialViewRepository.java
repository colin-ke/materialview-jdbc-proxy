package com.yy.jdbc.proxy.mview.common.manager;

import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.mview.common.Repository;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

/**
 * 物化视图JDBC操作接口
 *
 * @author ZhangXueJun
 */
public interface IMaterialViewRepository extends Repository<MaterialView> {

    /**
     * 根据事实表获取相关的物化视图
     *
     *
     * @param statement
     * @param factTable
     * @return
     */
    Collection<MaterialView> getMaterialViewListByFactTable(
            Statement statement, String factTable) throws SQLException;

    /**
     * 查找所有视图, 用于第三方使用
     * <p/>
     * 类似命令： show tables
     *
     * @param statement
     * @return
     * @throws SQLException
     */
    boolean showMaterialViews(Statement statement) throws SQLException;

    /**
     * 通过物化视图名称查找出所有能被用来rewrite的物化视图原始创建语句， , 用于第三方使用
     * <p/>
     * 类似命令：show create table tableName
     *
     * @param statement
     * @param viewName
     * @return
     * @throws SQLException
     */
    boolean showCreateMaterialView(Statement statement, String viewName) throws SQLException;
}
