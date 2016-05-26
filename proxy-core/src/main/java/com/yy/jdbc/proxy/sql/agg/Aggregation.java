package com.yy.jdbc.proxy.sql.agg;

import com.yy.jdbc.proxy.sql.Selectable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public abstract class Aggregation implements Selectable {

	String alias;
	Selectable target;

	protected Aggregation() {
	}

	public Aggregation(Selectable target) {
		this.target = target;
		alias = getAggName() + "_" + target.aliasOrName();
	}

	public Aggregation withAlias(String alias) {
		this.alias = alias;
		return this;
	}

	@Override
	public String aliasOrName() {
		return null == alias ? getAggName() + "_" + target.aliasOrName() : alias;
	}

	public Selectable getTarget() {
		return target;
	}

	public Selectable rewrite(String table, String name, boolean reAgg) {
		getField().rewrite(table, name, reAgg);
		return this;
	}

	@Override
	public boolean isAgg() {
		return true;
	}

	@Override
	public String toSql() {
		return getAggName() + "(" + target.toSql() + ")" + (null == alias ? "" : " AS " + alias);
	}

	@Override
	public String toSqlNoAlias() {
		return getAggName() + "(" + target.toSqlNoAlias() + ")";
	}

	@Override
	public String toString() {
		return toSql();
	}

	abstract protected String getAggName();

	@Override
	public Selectable getField() {
		return target.getField();
	}

	@Override
	public String getAlias() {
		return alias;
	}

	@Override
	public int hashCode() {
		return toSqlNoAlias().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(null == obj)
			return false;
		if(this == obj)
			return true;
		if(obj instanceof Aggregation) {
			return getAggName().equals(((Aggregation) obj).getAggName()) && target.equals(((Aggregation) obj).getTarget());
		} else {
			return false;
		}
	}

}
