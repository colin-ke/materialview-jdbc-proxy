package com.yy.jdbc.proxy.mview.common;

import java.sql.Connection;
import java.sql.SQLException;
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
     * @param conn
     * @param id
     * @return
     */
    boolean isExists(Connection conn, String id) throws SQLException;

    /**
     * 创建此对象
     *
     * @param conn
     * @param t
     * @return
     */
    boolean create(Connection conn, T t) throws SQLException;

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
     * @param conn
     * @param id
     * @return
     */
    boolean delete(Connection conn, String id) throws SQLException;

    /**
     * 查询所有
     *
     * @param conn
     * @return
     */
    Collection<T> listAll(Connection conn) throws SQLException;

    /**
     * 根据id获取对象
     *
     * @param conn
     * @param id
     * @return
     */
    T get(Connection conn, String id) throws SQLException;
}
