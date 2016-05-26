package com.yy.jdbc.proxy.sql.parser.select;

import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TExpressionList;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.yy.jdbc.proxy.sql.where.field.FieldCondition;

public abstract class SQLExpression {
	public static final String SPACE=" ";
	protected EExpressionType exprType;
	protected TExpression expr;
	protected String operatorToken;
	protected String belongToTable;
	public Set<String> tableNameSet;
	//是否能作为单表过滤条件
	protected boolean isCanbePushDown;
	
	private static Map<EExpressionType,String> instanceType =
			new HashMap<EExpressionType,String>();
	
	static {
		instanceType.put(EExpressionType.simple_comparison_t, "com.yy.jdbc.proxy.sql.parser.expr.SimpleComparisonExpr");
		instanceType.put(EExpressionType.in_t, "com.yy.jdbc.proxy.sql.parser.expr.InExpr");
		instanceType.put(EExpressionType.logical_or_t, "com.yy.jdbc.proxy.sql.parser.expr.OrExpr");
		instanceType.put(EExpressionType.simple_object_name_t, "com.yy.jdbc.proxy.sql.parser.expr.ObjectNameExpr");
		instanceType.put(EExpressionType.function_t, "com.yy.jdbc.proxy.sql.parser.expr.FuncExpr");
		instanceType.put(EExpressionType.null_t, "com.yy.jdbc.proxy.sql.parser.expr.NullExpr");
		instanceType.put(EExpressionType.unknown_t, "com.yy.jdbc.proxy.sql.parser.expr.DefaultExpr");
	}
	
	public SQLExpression(EExpressionType exprType,String operatorToken,TExpression expr){
		this.exprType=exprType;
		this.operatorToken=operatorToken;
		this.expr=expr;
	}
	
	/**
	 *得到expr下面的每个字段，带表前缀,参数不为空表示所有字段来源于指定的某个表，
	 *此时，每个字段的前缀都是这个参数名字
	 *
	 * @throws SQLException 
	 * @param  defaultTableName
	 * */
	public abstract Multimap<String, String> getFieldsWithTablePrefix() throws SQLException;
	
	/**
	 * 得到expr下面的每个字段，不带表前缀，字段是不是默认属于一个表的
	 * @throws SQLException 
	 * @param defaultTableName
	 * */
	public Multimap<String, String> getFieldsWithoutTablePrefix() throws SQLException{
		throw new SQLException("当前表达式："+expr+"不支持获取带前缀的表字段");
	}
	
	public abstract void formatField(String tableName) throws SQLException;

	public abstract FieldCondition<String> getValue(Map<String, String> aliasWithRealTableName) throws SQLException;
	
	protected Multimap<String,String> getTableFieldsWithTablePrefix(TExpression funcExpr) throws SQLException{
		Multimap<String,String> tableFieldsWithTablePrefix=HashMultimap.create();
		Set<TExpression> objectNames=getObjectNameFromFuncExpr(funcExpr);
		for(TExpression objectName:objectNames){
			Entry<String, String> tableFieldEntry=SQLUtil.getTableNameAndFieldName(objectName);
			String tableName=tableFieldEntry.getKey();
			String field=tableFieldEntry.getValue();
			tableFieldsWithTablePrefix.put(tableName,field);
		}
		return tableFieldsWithTablePrefix;
	}
	
	
	protected Multimap<String,String> getTableFieldsWithoutTablePrefix(TExpression funcExpr) throws SQLException{
		Multimap<String,String> tableFieldsWithoutTablePrefix=HashMultimap.create();
		Set<TExpression> objectNames=getObjectNameFromFuncExpr(funcExpr);
		for(TExpression objectName:objectNames){
			Entry<String, String> tableFieldEntry=SQLUtil.getTableNameAndFieldName(objectName);
			tableFieldsWithoutTablePrefix.put(tableFieldEntry.getKey(),tableFieldEntry.getValue());
		}
		return tableFieldsWithoutTablePrefix;
	}
	
	
	/**
	 * 递归获取每个表的字段，前缀为空的话，使用defaultTableName代替
	 * 必须是字段或者字段的函数表达式，暂时不支持例如+、-等操作符
	 * 
	 * */
	protected Set<TExpression> getObjectNameFromFuncExpr(TExpression funcExpr) throws SQLException {
		Set<TExpression> objectNameSet=new HashSet<>();
		if(isSimpleObjectNameExpr(funcExpr)){
			objectNameSet.add(funcExpr);
		}else if (isFuncCallExpr(funcExpr)) {
			TExpressionList argExprList = funcExpr.getFunctionCall().getArgs();
			for (int i = 0; i < argExprList.size(); i++) {
				TExpression funcArgExpr = argExprList.getExpression(i);
				objectNameSet.addAll(getObjectNameFromFuncExpr(funcArgExpr));
			}
		}else if(isParenthesisExpr(funcExpr)){
			objectNameSet.addAll(getObjectNameFromFuncExpr(funcExpr.getLeftOperand()));
		}else if(isSimpleConstantExpr(funcExpr)){
		}else{
				throw new SQLException(funcExpr+"不支持操作符:"+funcExpr.getOperatorToken());
		}
		return objectNameSet;
	}

	public EExpressionType getExprType() {
		return exprType;
	}

	public TExpression getExpr() {
		return expr;
	}
	
	@Override
	public String toString(){
		return expr.toString();
	}
	
	@Override
	public boolean equals(Object object){
		if(object==null)return false;
		if(!(object instanceof String))return false;
		if(object.toString().equals(this.toString())){
			return true;
		}else{
			return false;
		}
	}
	@Override
	public int hashCode(){
		return this.expr.toString().hashCode();
	}
	

	public static boolean isSimpleObjectNameExpr(TExpression expr){
		return EExpressionType.simple_object_name_t.equals(expr.getExpressionType());
	}
	public static boolean isFuncCallExpr(TExpression expr){
		return EExpressionType.function_t.equals(expr.getExpressionType());
	}
	public static boolean isSimpleConstantExpr(TExpression expr){
		return EExpressionType.simple_constant_t.equals(expr.getExpressionType());
	}
	public static boolean isOrExpr(TExpression expr){
		return EExpressionType.logical_or_t.equals(expr.getExpressionType());
	}
	public static boolean isAndExpr(TExpression expr){
		return EExpressionType.logical_and_t.equals(expr.getExpressionType());
	}
	public static boolean isParenthesisExpr(TExpression expr){
		return EExpressionType.parenthesis_t.equals(expr.getExpressionType());
	}
	
	
	@SuppressWarnings("unchecked")
	public static SQLExpression getExpressionInstance(TExpression expr) throws SQLException {
		try{
			String clazz=instanceType.get(expr.getExpressionType());
			if(clazz==null){
				clazz=instanceType.get(EExpressionType.unknown_t);
			}
			Class<SQLExpression> resolveClass =(Class<SQLExpression>) Class
					.forName(clazz);
			Constructor<SQLExpression> instanceTypeConstructor=resolveClass.getConstructor(TExpression.class);
			SQLExpression resultClass=instanceTypeConstructor.newInstance(expr);
			return resultClass;
		} catch (Exception e) {
			throw new SQLException(e.getCause()+","+e.getMessage());
		}
		
	}

}
