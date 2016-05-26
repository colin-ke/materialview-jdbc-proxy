package com.yy.jdbc.proxy.mview.common;

import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基础工具类
 *
 * @author ZhangXueJun
 */
public abstract class BaseDAO {

    /**
     * SQL运行工具类
     */
    protected QueryRunner runner = new QueryRunner();

    /**
     * 通用日志
     */
    protected Logger logger = LoggerFactory.getLogger(getClass());

}
