package com.yy.jdbc.proxy.sql.agg;

import com.yy.jdbc.proxy.sql.Selectable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class Max extends Aggregation {
	public Max(Selectable target) {
		super(target);
	}

	@Override
	protected String getAggName() {
		return "max";
	}

	@Override
	public boolean associative() {
		return target.associative();
	}

	@Override
	public Selectable copy() {
		return new Max(target.copy()).withAlias(alias);
	}
}
