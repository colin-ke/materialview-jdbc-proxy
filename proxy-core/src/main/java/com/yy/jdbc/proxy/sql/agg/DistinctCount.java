package com.yy.jdbc.proxy.sql.agg;

import com.yy.jdbc.proxy.sql.Selectable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class DistinctCount extends Aggregation {

	public DistinctCount(Selectable target) {
		super(target);
	}

	@Override
	public String toSql() {
		StringBuilder sb = new StringBuilder("count(distinct ");
		sb.append(target.toSql()).append(")");
		if(null != alias) {
			sb.append(" AS ").append(alias);
		}
		return sb.toString();
	}

	@Override
	public String toSqlNoAlias() {
		return "count (distinct " + target.toSql() + ")";
	}

	@Override
	protected String getAggName() {
		return "count_distinct";
	}

	@Override
	public boolean associative() {
		return false;
	}

	@Override
	public Selectable copy() {
		return new DistinctCount(target.copy()).withAlias(alias);
	}
}
