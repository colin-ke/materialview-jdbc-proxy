package com.yy.jdbc.proxy.sql.agg;

import com.yy.jdbc.proxy.sql.Selectable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class Count extends Aggregation {

	public Count(Selectable target) {
		super(target);
	}

	@Override
	protected String getAggName() {
		return "count";
	}

	@Override
	public boolean associative() {
		return target.associative();
	}

	@Override
	public Selectable rewrite(String table, String name, boolean reAgg) {
		if(reAgg) {
			return new Sum(target.copy().rewrite(table, name, reAgg)).withAlias(getAlias());
		} else
			return super.rewrite(table, name, reAgg);
	}

	@Override
	public Selectable copy() {
		return new Count(target.copy()).withAlias(alias);
	}
}
