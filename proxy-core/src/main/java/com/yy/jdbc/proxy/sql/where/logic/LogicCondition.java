package com.yy.jdbc.proxy.sql.where.logic;

import com.google.common.collect.ImmutableList;
import com.yy.jdbc.proxy.sql.Selectable;
import com.yy.jdbc.proxy.sql.where.Condition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author colin.ke keqinwu@yy.com
 */
public abstract class LogicCondition implements Condition {

	protected final Condition cdt1;
	protected final Condition cdt2;

	public LogicCondition(Condition cdt1, Condition cdt2) {
		this.cdt1 = cdt1;
		this.cdt2 = cdt2;
	}

	public Condition[] decompose() {
		return new Condition[]{cdt1, cdt2};
	}

	/**
	 * 判定该条件是否包含另一条件时，是否需要本条件中每一个子条件都包含才能决定包含
	 */
	abstract protected boolean needFullContain();

	@Override
	public List<Selectable> getSelectable() {
		List<Selectable> list = new ArrayList<>(cdt1.getSelectable());
		list.addAll(cdt2.getSelectable());
		return list;
	}
}
