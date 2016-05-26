package com.yy.jdbc.proxy.sql;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class Field implements Selectable {
	String table;
	String name;
	String alias;

	public Field(String table, String name) {
		this.table = table;
		this.name = name;
	}

	public Field withAlias(String alias) {
		this.alias = alias;
		return this;
	}

	public Selectable rewrite(String table, String name, boolean reAgg) {
		this.table = table;
		this.name = name;
		return this;
	}

	@Override
	public Selectable copy() {
		return new Field(table, name).withAlias(alias);
	}

	@Override
	public boolean isAgg() {
		return false;
	}

	@Override
	public String toSql() {
		return toSqlNoAlias() + (null == alias ? "" : " AS " + alias);
	}

	@Override
	public String toSqlNoAlias() {
		return table + "." + name;
	}

	@Override
	public Selectable getField() {
		return this;
	}

	@Override
	public String getAlias() {
		return alias;
	}

	@Override
	public String aliasOrName() {
		return null == alias ? name : alias;
	}

	@Override
	public boolean associative() {
		return true;
	}

	@Override
	public String toString() {
		return toSql();
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
		if(!(obj instanceof Field))
			return false;
		Field theField = (Field) obj;
		return this.table.equals(theField.table) && this.name.equals(theField.name);
	}
}
