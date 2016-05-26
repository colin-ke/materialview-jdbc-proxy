package com.yy.jdbc.proxy.sql.parser.select;

import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TGroupBy;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.sql.SQLException;


/**
 * filterNode节点里仅仅包含的信息有两种，
 * 1)当前sql那些不能被任意一个表当成过滤条件的表达式
 * 2)能够被当前sql某个表当成过滤的表达式
 * */
public class FilterNode implements TreeNode {
	private TSelectSqlStatement stmt;
	private ExpressionNode havingExprNode;
	private ExpressionNode whereExprNode;
	public FilterNode(TSelectSqlStatement stmt) throws SQLException {
		this.stmt=stmt;
		this.init();
	}

	@Override
	public void init()throws SQLException  {
		String tableName=null;
		if(stmt.tables.size()==1){
			tableName=SQLUtil.getTableName(stmt.tables.getTable(0));
		}
		TGroupBy groupBy=stmt.getGroupByClause();
		if(groupBy!=null&&groupBy.getHAVING()!=null){
			TExpression havingExpr=groupBy.getHavingClause();
			havingExprNode=ExpressionNode.getExpressionsFromExpr(havingExpr,tableName);
		}
		if(stmt.getWhereClause()!=null){
			TExpression whereExpr=stmt.getWhereClause().getCondition();
			whereExprNode=ExpressionNode.getExpressionsFromExpr(whereExpr,tableName);
		}
	}

	public ExpressionNode getHavingExprNode() {
		return havingExprNode;
	}

	public void setHavingExprNode(ExpressionNode havingExprNode) {
		this.havingExprNode = havingExprNode;
	}

	public ExpressionNode getWhereExprNode() {
		return whereExprNode;
	}

	public void setWhereExprNode(ExpressionNode whereExprNode) {
		this.whereExprNode = whereExprNode;
	}
	
	
}
