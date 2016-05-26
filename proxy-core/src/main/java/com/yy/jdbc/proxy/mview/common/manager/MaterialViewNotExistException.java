package com.yy.jdbc.proxy.mview.common.manager;

import java.sql.SQLException;

/**
 * 物化视图已存在异常
 *
 * @author ZhangXueJun
 */
public class MaterialViewNotExistException extends SQLException {

    public MaterialViewNotExistException(String msg) {
        super("物化视图[" + msg + "]不存在！");
    }
}
