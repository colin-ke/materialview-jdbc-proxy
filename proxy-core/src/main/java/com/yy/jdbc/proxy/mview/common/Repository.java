package com.yy.jdbc.proxy.mview.common;


import com.yy.jdbc.proxy.mview.common.manager.MaterialViewAlreadyExistException;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
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
     * @param id
     * @return
     */
    boolean isExists(Connection conn, String id) throws SQLException;

    /**
     * 创建对象
     *
     * @return
     */
    boolean create(Connection conn, T t) throws SQLException;

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
     * @param id
     * @return
     */
    boolean delete(Connection conn, String id) throws SQLException;

    /**
     * 罗列已有的对象
     *
     * @return
     */
    Collection<T> listAll(Connection conn) throws SQLException;

    /**
     * 根据id获取对象
     *
     * @param id
     * @return
     */
    T get(Connection conn, String id) throws SQLException;
}