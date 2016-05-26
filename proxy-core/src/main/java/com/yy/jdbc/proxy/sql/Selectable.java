package com.yy.jdbc.proxy.sql;

import java.io.Serializable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public interface Selectable extends Serializable {

	/**
	 * is field(true) or agg(false).
	 * @return
	 */
	boolean isAgg();

	/**
	 *  alias or the field name.
	 * @return
	 */
	String toSql();

	String toSqlNoAlias();

	Selectable getField();

	String getAlias();

	String aliasOrName();

	/**
	 * 汇聚函数是否可拆分的(结合性)
	 *
	 * @return true  函数具有结合性(可拆分)
	 * false 函数不具有结核性(不可拆分)
	 */
	boolean associative();

	Selectable rewrite(String table, String name, boolean reAgg);

	Selectable copy();

	Selectable withAlias(String alias);
}
