package com.yy.jdbc.proxy.sql.parser.expr;

import gudusoft.gsqlparser.TSourceToken;
import gudusoft.gsqlparser.nodes.TExpression;

import java.sql.SQLException;
import java.util.Map;

import com.google.common.collect.Multimap;
import com.yy.jdbc.proxy.sql.parser.select.SQLExpression;
import com.yy.jdbc.proxy.sql.where.field.FieldCondition;

public class ObjectNameExpr extends SQLExpression {

	public ObjectNameExpr(TExpression expr) {
		super(expr.getExpressionType(), null, expr);
		if (expr.getObjectOperand().getObjectString() != null) {
			this.belongToTable=expr.getObjectOperand().getObjectString();
		}
	}

	@Override
	public Multimap<String, String> getFieldsWithTablePrefix()
			throws SQLException {
		Multimap<String, String> resultMap = getTableFieldsWithTablePrefix(expr);
		return resultMap;
	}

	@Override
	public Multimap<String, String> getFieldsWithoutTablePrefix()
			throws SQLException {
		Multimap<String, String> resultMap = getTableFieldsWithoutTablePrefix(expr);
		return resultMap;
	}

	@Override
	public void formatField(String tableName) throws SQLException{
		if(tableName==null)return;
		if (expr.getObjectOperand().getObjectString() == null) {
				TSourceToken tSourceToken = new TSourceToken(tableName + "."
						+ expr.getObjectOperand().getPartString());
				tSourceToken.container = expr.getStartToken().container;
				tSourceToken.posinlist = expr.getStartToken().posinlist;
				expr.getStartToken().container.add(tSourceToken.posinlist,
						tSourceToken);
				expr.getStartToken().container
						.remove(expr.getStartToken().posinlist + 1);
		}
	}

	@Override
	public FieldCondition<String> getValue(
			Map<String, String> aliasWithRealTableName) throws SQLException {
		throw new SQLException("不支持表达式"+this.expr);
	}

}