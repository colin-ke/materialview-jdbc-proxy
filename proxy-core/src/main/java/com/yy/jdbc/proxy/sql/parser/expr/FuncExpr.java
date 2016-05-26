package com.yy.jdbc.proxy.sql.parser.expr;

import gudusoft.gsqlparser.nodes.TExpression;

import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Multimap;
import com.yy.jdbc.proxy.sql.parser.select.SQLExpression;
import com.yy.jdbc.proxy.sql.where.field.FieldCondition;

public class FuncExpr extends SQLExpression {

	public FuncExpr(TExpression expr) {
		super(expr.getExpressionType(), null, expr);
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
	public void formatField(String tableName) throws SQLException {
		Set<TExpression> objectNames=getObjectNameFromFuncExpr(expr);
		for(TExpression objectName:objectNames){
			SQLExpression SQLExpression=getExpressionInstance(objectName);
			SQLExpression.formatField(tableName);
		}
	}

	@Override
	public FieldCondition<String> getValue(
			Map<String, String> aliasWithRealTableName) throws SQLException {
		throw new SQLException("不支持表达式"+this.expr);
	}
}
