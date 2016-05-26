package com.yy.jdbc.proxy.sql.parser.expr;

import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.nodes.TExpression;

import java.sql.SQLException;
import java.util.Map;

import com.google.common.collect.Multimap;
import com.yy.jdbc.proxy.sql.parser.select.SQLExpression;
import com.yy.jdbc.proxy.sql.where.field.FieldCondition;

public class NullExpr  extends SQLExpression {
	public NullExpr(TExpression expr) throws SQLException {
		super(EExpressionType.null_t, expr.getOperatorToken()
				.toString(), expr);
	}



	@Override
	public Multimap<String, String> getFieldsWithTablePrefix()
			throws SQLException {
		Multimap<String, String> resultMap = getTableFieldsWithTablePrefix(expr
				.getLeftOperand());
		return resultMap;
	}

	@Override
	public Multimap<String, String> getFieldsWithoutTablePrefix()
			throws SQLException {
		Multimap<String, String> resultMap = getTableFieldsWithoutTablePrefix(expr
				.getLeftOperand());
		return resultMap;
	}

	@Override
	public void formatField(String tableName) throws SQLException {
		SQLExpression.getExpressionInstance(expr.getLeftOperand())
				.formatField(tableName);
	}

	@Override
	public FieldCondition<String> getValue(
			Map<String, String> aliasWithRealTableName) throws SQLException {
		throw new SQLException("不支持表达式"+this.expr);
	}

}
