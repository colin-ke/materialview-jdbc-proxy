package com.yy.jdbc.proxy.sql.where;

import com.yy.jdbc.proxy.sql.Selectable;

import java.io.Serializable;
import java.util.List;

/**
 * @author colin.ke keqinwu@yy.com
 */
public interface Condition extends Serializable {

	/**
	 * 该条件是否属于（等于）目标条件
	 * @param condition 目标条件
	 * @return =0 完全相同
	 * 		   >0 属于
	 * 		   <0 不属于
	 */
	int belongs(Condition condition);

	String toSql();

	List<Selectable> getSelectable();
}
