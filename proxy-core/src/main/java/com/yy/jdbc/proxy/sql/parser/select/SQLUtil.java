package com.yy.jdbc.proxy.sql.parser.select;

import gudusoft.gsqlparser.nodes.TAliasClause;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TExpressionList;
import gudusoft.gsqlparser.nodes.TTable;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Maps;

public class SQLUtil {
	public static String getTableName(TTable table) {

		TAliasClause aliasClause=table.getAliasClause();
		if(table.getAliasClause()==null){
			return table.getName();
		}else{
			if(aliasClause==null){
				new IllegalArgumentException("子查询:"+table+"不存在别名,无法引用");
			}
			return aliasClause.getAliasName().getObjectString();
		}
	}

	

	
	 public static String replaceColumnByAlias(TExpression expr,
			Map<String, String> fieldWithAlias) throws SQLException{
		if(SQLExpression.isSimpleObjectNameExpr(expr)){
			String alias=fieldWithAlias.get(expr.toString());
			alias=alias==null?expr.toString():alias;
			return alias;
		}else if(SQLExpression.isSimpleConstantExpr(expr)){
			return expr.toString();
		}else if(SQLExpression.isFuncCallExpr(expr)) {
			String funcName=expr.getFunctionCall().getFunctionName().toString();
			TExpressionList argExprList = expr.getFunctionCall().getArgs();
			int argSize=argExprList.size();
			String[] args=new String[argSize];
			for (int i = 0; i < argSize; i++) {
				TExpression funcArgExpr = argExprList.getExpression(i);
				args[i]=replaceColumnByAlias(funcArgExpr,fieldWithAlias);
			}
			return funcName+"("+StringUtils.join(args,",")+")";			
		}else{
			throw new SQLException("函数内不支持表达式:"+expr.getOperatorToken());
		}
	}
	 public static TExpression removeParenthesis(TExpression expr) throws SQLException{
		 if(SQLExpression.isParenthesisExpr(expr)){
			expr=expr.getLeftOperand();
			return removeParenthesis(expr);
		 }else{
			 return expr;
		 }
	 }
	
	/**
	 * 判断expr是否有表前缀，如果没有，通过修改expr的startToken，把startToken的值设置为带前缀的
	 * field字段，弃用getObjectOperand方法，采用每次获取时，解析startToken字段。
	 * (修改解析后的token列表过于冗余，而且容易出错)
	 * @throws SQLException 
	 * */
	public static void formatField(TExpression expr,String tableName) throws SQLException{
		SQLExpression msqlExpression=SQLExpression.getExpressionInstance(expr);
		msqlExpression.formatField(tableName);
	}
	

	/**
	 * 调用每个MsqlExpression的formatField方法，如果字段没有表前缀，则加上表前缀，
	 * 并且设置这个表达式所属于的表是哪个
	 * */
	public static void formatField(List<SQLExpression> msqlExpressions,String tableName) throws SQLException{
		for(SQLExpression msqlExpression:msqlExpressions){
			msqlExpression.formatField(tableName);
		}
	}
	
	
	public static Entry<String, String> getTableNameAndFieldName(TExpression expr) throws SQLException{
		if(!SQLExpression.isSimpleObjectNameExpr(expr))
			throw new SQLException("无法直接从"+expr+"获取表名和字段名");
		String fieldFullName=expr.toString();
		int index=fieldFullName.indexOf(".");
		if(index==-1){
			throw new SQLException("字段需要指定表前缀:"+fieldFullName);
//			return Maps.immutableEntry(null, fieldFullName);
		}
		String tableName=fieldFullName.substring(0, index);
		String fieldName=fieldFullName.substring(index+1);
		return Maps.immutableEntry(tableName, fieldName);
	}

	public static <T> T getInstance(String clazz) throws SQLException {
		try{

			@SuppressWarnings("unchecked")
			Class<T> resolveClass =(Class<T>) Class
					.forName(clazz);
			Constructor<T> instanceTypeConstructor=resolveClass.getConstructor();
			T resultClass=instanceTypeConstructor.newInstance();
			return resultClass;
		} catch (Exception e) {
			throw new SQLException(e.getCause()+","+e.getMessage());
		}
	}

}
