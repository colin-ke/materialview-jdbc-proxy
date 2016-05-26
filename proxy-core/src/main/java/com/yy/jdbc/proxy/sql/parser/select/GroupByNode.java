package com.yy.jdbc.proxy.sql.parser.select;

import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TGroupBy;
import gudusoft.gsqlparser.nodes.TGroupByItemList;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GroupByNode implements TreeNode{
	private List<SQLExpression> groupByExpr;
	private TSelectSqlStatement stmt;

	public GroupByNode(TSelectSqlStatement stmt) throws SQLException {
		this.stmt=stmt;
		this.init();
	}
	@Override
	public void init() throws SQLException {
		TGroupBy groupBy=stmt.getGroupByClause();
		String tableName=null;
		if(stmt.tables.size()==1){
			tableName=SQLUtil.getTableName(stmt.tables.getTable(0));
		}
		if(groupBy!=null&&groupBy.getGROUP()!=null){
			TGroupByItemList groupbyList=stmt.getGroupByClause().getItems();
			int groupByListSize=groupbyList.size();
			this.groupByExpr=new ArrayList<>();
			for(int index=0;index<groupByListSize;index++){
				TExpression expr=groupbyList.getGroupByItem(index).getExpr();
				SQLUtil.formatField(expr, tableName);
				SQLExpression msqlExpression=SQLExpression.getExpressionInstance(expr);
				groupByExpr.add(msqlExpression);
			}
		}
	}
	
	public List<SQLExpression> getGroupByExpr() {
		return groupByExpr;
	}

}
