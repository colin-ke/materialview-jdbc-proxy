package com.yy.jdbc.proxy.sql.where.logic;

import com.yy.jdbc.proxy.sql.where.Condition;
import com.yy.jdbc.proxy.sql.where.ResultUtil;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class Or extends LogicCondition {

	public Or(Condition cdt1, Condition cdt2) {
		super(cdt1, cdt2);
	}

	@Override
	protected boolean needFullContain() {
		return false;
	}

	@Override
	public int belongs(Condition condition) {
		return ResultUtil.and(cdt1.belongs(condition), cdt2.belongs(condition));
	}

	@Override
	public String toSql() {
		return "(" + cdt1.toSql() + ") OR (" + cdt2.toSql() + ")";
	}
}
