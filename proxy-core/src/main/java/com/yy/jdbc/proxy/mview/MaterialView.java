package com.yy.jdbc.proxy.mview;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.yy.jdbc.proxy.sql.SqlExpression;

/**
 * 物化视图模型
 *
 * @author ZhangXueJun
 */
public class MaterialView implements Serializable {

    /**
     * 视图名字
     */
    private String viewName;

    /**
     * 视图构建方式，立即构建或者延迟构建(第一次刷新构建)
     */
    private Build build = Build.immediate;

    /**
     * 视图刷新方式，现在支持全量刷新
     */
    private Refresh refresh = Refresh.complete;

    /**
     * 对物化视图的事实表进行查询时，是否通过查询物化视图来得到结果，默认是开启
     */
    private boolean rewrite = true;

    /**
     * 物化视图操作的sql，包括创建创建、删除、刷新
     */
    private String sqlStr;

    /**
     * 事实表
     */
    private String factTable;

    /**
     * SQL解析后的数据结构
     */
    private SqlExpression sqlDataStruct;

    /**
     * 是否已存储
     */
    private boolean store = false;

    /**
     * 原始建表语句
     */
    private String createSQL;

	public String getViewName() {
        return viewName;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public Build getBuild() {
        return build;
    }

    public void setBuild(Build build) {
        this.build = build;
    }

    public Refresh getRefresh() {
        return refresh;
    }

    public void setRefresh(Refresh refresh) {
        this.refresh = refresh;
    }

    public boolean isRewrite() {
        return rewrite;
    }

    public void setRewrite(boolean rewrite) {
        this.rewrite = rewrite;
    }

    public String getSqlStr() {
        return sqlStr;
    }

    public void setSqlStr(String sqlStr) {
        this.sqlStr = sqlStr;
    }

    public String getFactTable() {
        return factTable;
    }

    public void setFactTable(String factTable) {
        this.factTable = factTable;
    }

    public SqlExpression getSqlDataStruct() {
        return sqlDataStruct;
    }

    public void setSqlDataStruct(SqlExpression sqlDataStruct) {
        this.sqlDataStruct = sqlDataStruct;
    }

    public boolean isStore() {
        return store;
    }

    public void setStore(boolean store) {
        this.store = store;
    }

    public String getCreateSQL() {
        return createSQL;
    }

    public void setCreateSQL(String createSQL) {
        this.createSQL = createSQL;
    }

    /**
     * 构建策略
     *
     * @author ZhangXueJun
     */
    public enum Build {

        /**
         * 在创建物化视图的时候就生成数据
         */
        @Default
        immediate,

        /**
         * 在创建时不生成数据
         */
        deferred;
    }

    /**
     * 默认方式
     *
     * @author ZhangXueJun
     */
    @Retention(RetentionPolicy.SOURCE)
    @Target(ElementType.FIELD)
    public @interface Default {
    }

    /**
     * 刷新策略
     *
     * @author ZhangXueJun
     */
    public enum Refresh {

        /**
         * 增量刷新，只刷新自上次刷新以后进行的修改
         */
        fast,

        /**
         * 对整个物化视图进行完全的刷新
         */
        @Default
        complete,

        /**
         * 如果可以快速刷新，就执行快速刷新；否则，执行完全刷新。
         */

        force,

        /**
         * 物化视图不进行任何刷新
         */
        never;
    }
}