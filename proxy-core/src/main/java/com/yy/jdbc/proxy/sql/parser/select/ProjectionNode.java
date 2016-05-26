package com.yy.jdbc.proxy.sql.parser.select;

import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TResultColumn;
import gudusoft.gsqlparser.nodes.TResultColumnList;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ArrayListMultimap;

public class ProjectionNode implements TreeNode{
	private TSelectSqlStatement sqlStmt;
	private TResultColumnList columnList;
	
	//select所有字段或字段表达式
	private List<SQLExpression> selectColumnsExpr;
	
	//每个表对应的多个字段(字段带表前缀)
	private ArrayListMultimap<String,String> fieldsWithTablePrefix;
	
	//每个表对应的多个字段(字段不带表前缀)
	private ArrayListMultimap<String, String> fieldsWithoutTablePrefix;
	
	//select后每个字段或者字段表达式显示指定的别名
	private Map<String, String> columnReferHintAlias;
	
	public ProjectionNode(TSelectSqlStatement sqlStmt) throws SQLException{
		this.sqlStmt=sqlStmt;
		this.columnList=sqlStmt.getResultColumnList();
		this.fieldsWithoutTablePrefix=ArrayListMultimap.create();
		this.selectColumnsExpr=new ArrayList<>();
		this.columnReferHintAlias=new HashMap<>();
		this.fieldsWithTablePrefix=ArrayListMultimap.create();
		init();
	}
	

	@Override
	public void init() throws SQLException {
		int columnSize=columnList.size();
		String tableName=null;
		//单表查询则表名字不变
		if(sqlStmt.tables.size()==1){
			tableName=SQLUtil.getTableName(sqlStmt.tables.getTable(0));
		}
		Map<String, String> unHintFieldAlias=new HashMap<>();
		
		//按照表名划分字段集
		for(int index=0;index<columnSize;index++){
			TResultColumn column=columnList.getResultColumn(index);
			TExpression expr=SQLUtil.removeParenthesis(column.getExpr());
			//单表查询,需要为每个没有前缀的字段加上前缀
			SQLUtil.formatField(expr, tableName);
			//出现任一非常量、非普通字段或非函数调用，则为非法
			checkIsSupportColumn(expr);
			selectColumnsExpr.add(SQLExpression.getExpressionInstance(expr));
			SQLExpression msqlExpression=SQLExpression.getExpressionInstance(expr);
			String alias=null;
			//column显示指定的别名
			if(column.getAliasClause()!=null){
				alias=column.getAliasClause().toString();
				this.columnReferHintAlias.put(expr.toString(),alias);
			}else{
				alias=StringUtils.join(msqlExpression.getFieldsWithTablePrefix().values(),"_").replaceAll("\\.","_");
				unHintFieldAlias.put(expr.toString(),alias);
			}
			fieldsWithoutTablePrefix.putAll(msqlExpression.getFieldsWithoutTablePrefix());
			fieldsWithTablePrefix.putAll(msqlExpression.getFieldsWithTablePrefix());
		}
		for(Entry<String, String> entry:unHintFieldAlias.entrySet()){
			int i=0;
			String unHintAlias=entry.getValue();
			while(this.columnReferHintAlias.containsValue(unHintAlias)){
				unHintAlias=unHintAlias+i++;
			}
			this.columnReferHintAlias.put(entry.getKey(),unHintAlias);
		}
	}
	
	
	private void checkIsSupportColumn(TExpression expr) throws SQLException{
		EExpressionType columnExprType=expr.getExpressionType();
		if(!ExpressionNode.isSupportOperator(columnExprType)){
			throw new SQLException("column不支持表达式:"+expr.getOperatorToken());
		}
	}

	public List<SQLExpression> getSelectColumnsExpr() {
		return selectColumnsExpr;
	}
	
	public List<String> getFieldsWithoutTablePrefix(String tableName){
		return fieldsWithoutTablePrefix.get(tableName);
	}

	public List<String> getFieldsWithTablePrefix(String tableName) {
		return fieldsWithTablePrefix.get(tableName);
	}


	public Map<String, String> getColumnReferHintAlias() {
		return columnReferHintAlias;
	}
	
}
