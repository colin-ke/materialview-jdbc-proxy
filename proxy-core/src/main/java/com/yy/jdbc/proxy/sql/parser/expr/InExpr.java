package com.yy.jdbc.proxy.sql.parser.expr;

import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TExpressionList;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Multimap;
import com.yy.jdbc.proxy.sql.Field;
import com.yy.jdbc.proxy.sql.parser.select.SQLExpression;
import com.yy.jdbc.proxy.sql.parser.select.SQLUtil;
import com.yy.jdbc.proxy.sql.where.field.FieldCondition;
import com.yy.jdbc.proxy.sql.where.field.Value;

public class InExpr extends SQLExpression {

	public InExpr(TExpression expr) throws SQLException {
		super(EExpressionType.in_t, (expr.getNotToken() == null ? "" : expr
				.getNotToken().toString().toLowerCase())
				+ expr.getOperatorToken().toString().toLowerCase(), expr);
	}

	@Override
	public Multimap<String, String> getFieldsWithTablePrefix()
			throws SQLException {
		Multimap<String, String> resultMap = getTableFieldsWithTablePrefix(expr
				.getLeftOperand());
		TExpressionList exprList = expr.getRightOperand().getExprList();
		int exprSize = exprList.size();
		for (int i = 0; i < exprSize; i++) {
			resultMap.putAll(getTableFieldsWithTablePrefix(exprList
					.getExpression(i)));
		}
		return resultMap;
	}

	@Override
	public Multimap<String, String> getFieldsWithoutTablePrefix()
			throws SQLException {
		Multimap<String, String> resultMap = getTableFieldsWithoutTablePrefix(expr
				.getLeftOperand());
		TExpressionList exprList = expr.getRightOperand().getExprList();
		int exprSize = exprList.size();
		for (int i = 0; i < exprSize; i++) {
			resultMap.putAll(getTableFieldsWithoutTablePrefix(exprList
					.getExpression(i)));
		}
		return resultMap;
	}

	@Override
	public void formatField(String tableName) throws SQLException {
		SQLExpression.getExpressionInstance(expr.getLeftOperand()).formatField(
				tableName);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public FieldCondition<String> getValue(Map<String, String> aliasWithRealTableName) throws SQLException {
		TExpressionList exprList = expr.getRightOperand().getExprList();
		List<Value<String>> values = new ArrayList<>();
		int exprSize = exprList.size();
		for (int i = 0; i < exprSize; i++) {
			TExpression expression=exprList.getExpression(i);
			values.add(new Value(expression.toString()));
		}
		Entry<String, String> entry=SQLUtil.getTableNameAndFieldName(this.expr.getLeftOperand());
		Field field=new Field(aliasWithRealTableName.get(entry.getKey()),entry.getValue());
		if(expr.getNotToken() == null){
			return FieldCondition.in(field, values.toArray(new Value[values.size()]));
		}else{
			return FieldCondition.notIn(field, values.toArray(new Value[values.size()]));
		}
	}

}