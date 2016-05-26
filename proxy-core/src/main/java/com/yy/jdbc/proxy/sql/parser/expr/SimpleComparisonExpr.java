package com.yy.jdbc.proxy.sql.parser.expr;

import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.nodes.TExpression;

import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Multimap;
import com.yy.jdbc.proxy.sql.Field;
import com.yy.jdbc.proxy.sql.parser.select.SQLExpression;
import com.yy.jdbc.proxy.sql.parser.select.SQLUtil;
import com.yy.jdbc.proxy.sql.where.field.FieldCondition;
import com.yy.jdbc.proxy.sql.where.field.Value;

public class SimpleComparisonExpr extends SQLExpression {
	public SimpleComparisonExpr(TExpression expr) throws SQLException {
		super(EExpressionType.simple_comparison_t, expr.getOperatorToken()
				.toString(), expr);
	}

	@Override
	public Multimap<String, String> getFieldsWithTablePrefix()
			throws SQLException {
		Multimap<String, String> resultMap = getTableFieldsWithTablePrefix(expr
				.getLeftOperand());
		resultMap.putAll(getTableFieldsWithTablePrefix(expr.getRightOperand()));
		return resultMap;
	}

	@Override
	public Multimap<String, String> getFieldsWithoutTablePrefix()
			throws SQLException {
		Multimap<String, String> resultMap = getTableFieldsWithoutTablePrefix(expr
				.getLeftOperand());
		resultMap.putAll(getTableFieldsWithoutTablePrefix(expr
				.getRightOperand()));
		return resultMap;
	}

	@Override
	public void formatField(String tableName) throws SQLException {
		SQLExpression.getExpressionInstance(expr.getLeftOperand()).formatField(
				tableName);
		SQLExpression.getExpressionInstance(expr.getRightOperand())
				.formatField(tableName);
	}

	@Override
	public FieldCondition<String> getValue(
			Map<String, String> aliasWithRealTableName) throws SQLException {
		String opStr = expr.getOperatorToken().toString();
		TExpression leftExpr = expr.getLeftOperand();
		TExpression rightExpr = expr.getRightOperand();
		if ((SQLExpression.isSimpleConstantExpr(leftExpr) && SQLExpression
				.isSimpleObjectNameExpr(rightExpr))
				|| (SQLExpression.isSimpleConstantExpr(rightExpr) && SQLExpression
						.isSimpleObjectNameExpr(leftExpr))) {

			if (SQLExpression.isSimpleConstantExpr(leftExpr)) {
				TExpression tmpExpr = leftExpr;
				leftExpr = rightExpr;
				rightExpr = tmpExpr;
			}
			Field field = getField(aliasWithRealTableName, leftExpr);
			Value value = new Value(rightExpr.toString());
			switch (opStr) {
			case "=":
				return FieldCondition.eq(field, value);
			case "!=":
				return FieldCondition.notEq(field, value);
			case ">=":
				return FieldCondition.gte(field, value);
			case ">":
				return FieldCondition.gt(field, value);
			case "<=":
				return FieldCondition.lte(field, value);
			case "<":
				return FieldCondition.lt(field, value);
			default:
				throw new SQLException("不支持表达式:" + this.expr);
			}
		} else {
			throw new SQLException("条件里的字段不支持函数");
		}
	}

	private Field getField(Map<String, String> aliasWithRealTableName,
			TExpression expr) throws SQLException {

		Entry<String, String> entry = SQLUtil.getTableNameAndFieldName(expr);
		return new Field(aliasWithRealTableName.get(entry.getKey()),
				entry.getValue());

	}
}