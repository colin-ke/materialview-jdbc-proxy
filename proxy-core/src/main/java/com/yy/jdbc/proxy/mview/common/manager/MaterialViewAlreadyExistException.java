package com.yy.jdbc.proxy.mview.common.manager;

import java.sql.SQLException;

/**
 * 物化视图已存在异常
 *
 * @author ZhangXueJun
 */
public class MaterialViewAlreadyExistException extends SQLException {

    public MaterialViewAlreadyExistException(String msg) {
        super("物化视图[" + msg + "]已存在！");
    }
}
