package com.yy.jdbc.proxy.sql.agg;

import com.yy.jdbc.proxy.sql.Selectable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class Avg extends Aggregation {

	public Avg(Selectable target) {
		super(target);
	}

	@Override
	protected String getAggName() {
		return "avg";
	}

	@Override
	public boolean associative() {
		return false;
	}

	@Override
	public Selectable copy() {
		return new Avg(target.copy()).withAlias(alias);
	}
}
