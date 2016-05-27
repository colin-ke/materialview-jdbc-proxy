package com.yy.jdbc.proxy.mview.common;


import com.yy.jdbc.proxy.mview.MaterialView;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

/**
 * 抽象数据层接口
 *
 * @author ZhangXueJun
 */
public interface Repository<T> {

    /**
     * 是否已存在此对象
     *
     *
     * @param statement
     * @param id
     * @return
     */
    boolean isExists(Statement statement, String id) throws SQLException;

    /**
     * 创建对象
     *
     * @return
     * @param statement
     * @param t
     */
    boolean create(Statement statement, MaterialView t) throws SQLException;

    /**
     * 更新对象
     *
     * @param t
     * @return
     * @throws SQLException
     */
//    boolean update(Connection conn, T t) throws SQLException;

    /**
     * 删除对象
     *
     *
     * @param statement
     * @param id
     * @return
     */
    boolean delete(Statement statement, String id) throws SQLException;

    /**
     * 罗列已有的对象
     *
     * @return
     * @param statement
     */
    Collection<T> listAll(Statement statement) throws SQLException;

    /**
     * 根据id获取对象
     *
     *
     * @param statement
     * @param id
     * @return
     */
    T get(Statement statement, String id) throws SQLException;
}