package com.yy.jdbc.proxy.sql.agg;

import com.yy.jdbc.proxy.sql.Selectable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class Sum extends Aggregation {

	public Sum(Selectable target) {
		super(target);
	}

	@Override
	protected String getAggName() {
		return "sum";
	}

	@Override
	public boolean associative() {
		return target.associative();
	}

	@Override
	public Selectable copy() {
		return new Sum(target.copy()).withAlias(alias);
	}
}