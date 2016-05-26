package com.yy.jdbc.proxy.sql.parser.select;

import gudusoft.gsqlparser.nodes.TTable;

import java.sql.SQLException;

public class FromNode implements TreeNode {
	private TreeNode parentNode;
	private String alias;
	private TTable table;
	private String tableName;

	public FromNode(TTable table,TreeNode parentNode) throws SQLException {
		this.table=table;
		this.parentNode=parentNode;
		init();
	}
	
	@Override
	public void init()throws SQLException  {
		if(table.getSubquery()!=null){
			throw new SQLException("sql不支持子查询");
		}
		this.alias=SQLUtil.getTableName(table);
		this.tableName=table.getName();
	}

	public String getAlias() {
		return alias;
	}

	public String getTableName(){
		return this.tableName;
	}
	
	public String getTableNameWithAlias(){
		if(this.alias.equals(this.tableName)){
			return alias;
		}else{
			return this.tableName+" "+this.alias;
		}
	}

	public TreeNode getParentNode() {
		return parentNode;
	}

	public void setParentNode(TreeNode parentNode) {
		this.parentNode = parentNode;
	}

}
