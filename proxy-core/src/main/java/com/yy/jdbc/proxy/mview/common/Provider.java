package com.yy.jdbc.proxy.mview.common;

import com.yy.jdbc.proxy.mview.MaterialView;

import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

/**
 * 抽象服务接口
 *
 * @author ZhangXueJun
 */
public interface Provider<T> {

    /**
     * 是否存在
     *
     * @param statement
     * @param id
     * @return
     */
    boolean isExists(Statement statement, String id) throws SQLException;

    /**
     * 创建此对象
     *
     * @param statement
     * @param t
     * @return
     */
    boolean create(Statement statement, MaterialView t) throws SQLException;

    /**
     * 更新此对象
     *
     * @param t
     * @return
     */
//    boolean update(T t);

    /**
     * 删除此对象
     *
     * @param statement
     * @param id
     * @return
     */
    boolean delete(Statement statement, String id) throws SQLException;

    /**
     * 查询所有
     *
     * @param statement
     * @return
     */
    Collection<T> listAll(Statement statement) throws SQLException;

    /**
     * 根据id获取对象
     *
     * @param statement
     * @param id
     * @return
     */
    T get(Statement statement, String id) throws SQLException;
}
