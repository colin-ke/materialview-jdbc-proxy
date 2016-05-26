package com.yy.jdbc.proxy.sql;

import java.io.Serializable;

/**
 * @author colin.ke keqinwu@yy.com
 */
public class OrderBy implements Serializable {

	public static enum Order {
		asc, desc
	}

	Selectable field;
	Order order;

	public OrderBy(Selectable field, Order order) {
		this.field = field.copy().withAlias(null);
		this.order = order;
	}

	public String toSql() {
		return field.toSql() + " " + order.name();
	}

	@Override
	public String toString() {
		return toSql();
	}

	public Selectable getField() {
		return field;
	}

	public Order getOrder() {
		return order;
	}

	public static OrderBy asc(Field field) {
		return new OrderBy(field, Order.asc);
	}

	public static OrderBy desc(Field field) {
		return new OrderBy(field, Order.desc);
	}
}
