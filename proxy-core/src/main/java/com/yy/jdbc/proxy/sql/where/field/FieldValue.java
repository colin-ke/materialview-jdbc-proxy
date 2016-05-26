package com.yy.jdbc.proxy.sql.where.field;

import com.yy.jdbc.proxy.sql.RelationComparable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public interface FieldValue<V extends Comparable<V>> extends RelationComparable<FieldValue<V>> {


	boolean isRange();

	Iterable<Range<V>> getRange();

	Iterable<Value<V>> getValues();

}
