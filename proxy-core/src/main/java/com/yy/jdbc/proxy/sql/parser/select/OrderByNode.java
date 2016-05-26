package com.yy.jdbc.proxy.sql.parser.select;

import gudusoft.gsqlparser.nodes.TOrderBy;
import gudusoft.gsqlparser.nodes.TOrderByItem;
import gudusoft.gsqlparser.nodes.TOrderByItemList;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


public class OrderByNode implements TreeNode{
	private TSelectSqlStatement stmt;
	private List<Pair<SQLExpression, String>> colAndOrderByType;

	public OrderByNode(TSelectSqlStatement stmt) throws SQLException {
		this.stmt=stmt;
		this.init();
	}
	@Override
	public void init() throws SQLException {
		TOrderBy orderBy=stmt.getOrderbyClause();
		String tableName=null;
		if(stmt.tables.size()==1){
			tableName=SQLUtil.getTableName(stmt.tables.getTable(0));
		}
		if(orderBy!=null&&orderBy.getItems()!=null){
			TOrderByItemList orderByItems=orderBy.getItems();
			int orderByListSize=orderByItems.size();
			this.colAndOrderByType=new ArrayList<>();
			for(int index=0;index<orderByListSize;index++){
				TOrderByItem expr=orderByItems.getOrderByItem(index);
				SQLUtil.formatField(expr.getSortKey(), tableName);
				SQLExpression msqlExpression=SQLExpression.getExpressionInstance(expr.getSortKey());
				colAndOrderByType.add(new ImmutablePair<SQLExpression,String>(msqlExpression,expr.getSortType()==1?"asc":"desc"));
			}
		}
	}
	public List<Pair<SQLExpression, String>> getColAndOrderByType() {
		return colAndOrderByType;
	}
	public void setColAndOrderByType(
			List<Pair<SQLExpression, String>> colAndOrderByType) {
		this.colAndOrderByType = colAndOrderByType;
	}

}
