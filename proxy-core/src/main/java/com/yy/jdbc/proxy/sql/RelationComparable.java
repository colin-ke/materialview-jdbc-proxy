package com.yy.jdbc.proxy.sql;

import com.yy.jdbc.proxy.sql.where.field.Relation;

import java.io.Serializable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public interface RelationComparable<T> extends Serializable {

	Relation relationWith(T obj);
}
