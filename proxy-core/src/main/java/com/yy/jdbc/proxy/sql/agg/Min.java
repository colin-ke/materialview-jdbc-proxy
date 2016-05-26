package com.yy.jdbc.proxy.sql.agg;

import com.yy.jdbc.proxy.sql.Selectable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class Min extends Aggregation {

	public Min(Selectable target) {
		super(target);
	}

	@Override
	protected String getAggName() {
		return "min";
	}

	@Override
	public boolean associative() {
		return target.associative();
	}

	@Override
	public Selectable copy() {
		return new Min(target.copy()).withAlias(alias);
	}
}