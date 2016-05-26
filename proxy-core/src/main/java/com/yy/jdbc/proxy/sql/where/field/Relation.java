package com.yy.jdbc.proxy.sql.where.field;

/**
 * @author colin.ke keqinwu@yy.com
 */
public enum Relation {
	CONTAINS,
    BELONGS,
    EQUAL,
    UNKNOWN, // 既不包含，也不属于，更不等于
    SEPARATE, // 相离（range）
    CROSS; // 相交（range）
}
