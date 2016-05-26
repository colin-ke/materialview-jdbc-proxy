package com.yy.jdbc.proxy.sql.parser.select;

import gudusoft.gsqlparser.EJoinType;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

public class JoinNode implements TreeNode{
	private EJoinType joinType;
	private JoinNode parentNode;
	private JoinNode childNode;
	
	
	private FromNode leftTable;
	private FromNode rightTable;
	private String factTable;
	
	private ExpressionNode onConditionNode;
	
	public JoinNode(EJoinType joinType,FromNode leftTable,
			FromNode rightTable,ExpressionNode onConditionNode,
			JoinNode parentNode) throws SQLException {
		this.joinType=joinType;
		this.leftTable=leftTable;
		this.rightTable=rightTable;
		this.onConditionNode=onConditionNode;
		this.parentNode=parentNode;
		this.init();
	}
	
	public JoinNode(EJoinType joinType,JoinNode leftJoinNode,
			FromNode rightTable,ExpressionNode onConditionNode) throws SQLException {
		this.joinType=joinType;
		this.rightTable=rightTable;
		this.onConditionNode=onConditionNode;
		this.parentNode=leftJoinNode;
		this.init();
	}


	@Override
	public void init() {

		
	}
	
	public String getJoinNameString(){
		return joinType+" join";
	}


	public EJoinType getJoinType() {
		return joinType;
	}

	public void setJoinType(EJoinType joinType) {
		this.joinType = joinType;
	}

	public FromNode getLeftTable() {
		return leftTable;
	}

	public void setLeftTable(FromNode leftTable) {
		this.leftTable = leftTable;
	}

	public FromNode getRightTable() {
		return rightTable;
	}

	public void setRightTable(FromNode rightTable) {
		this.rightTable = rightTable;
	}

	public ExpressionNode getOnConditionNode() {
		return onConditionNode;
	}

	public void setOnConditionNode(ExpressionNode onConditionNode) {
		this.onConditionNode = onConditionNode;
	}


	public TreeNode getParentNode() {
		return parentNode;
	}

	public void setParentNode(JoinNode parentNode) {
		this.parentNode = parentNode;
	}
	public JoinNode getChildNode() {
		return childNode;
	}

	public void setChildNode(JoinNode childNode) {
		this.childNode = childNode;
	}

	public boolean isStartModel() throws SQLException{
		if(parentNode!=null){
			return false;
		}
		Set<String> tableNames=getJoinTables(this.onConditionNode);
		String factTable=null;
		if(tableNames.size()!=2)return false;
		JoinNode joinChildNode=this.childNode;
		if(joinChildNode==null){
			this.factTable=this.leftTable.getAlias();
			return true;
		}else{
			while(joinChildNode!=null){
				Set<String> childJoinTables=joinChildNode.getJoinTables(joinChildNode.getOnConditionNode());
				if(childJoinTables.size()!=2)return false;
				else{
					@SuppressWarnings("unchecked")
					Iterator<String> iter=CollectionUtils.intersection(tableNames, childJoinTables).iterator();
					if(iter.hasNext()){
						String factTableTmp=iter.next();
						if(factTable!=null&&!StringUtils.equals(factTable,factTableTmp)){
							return false;
						}
						factTable=factTableTmp;
					}
					else return false;
					joinChildNode=joinChildNode.getChildNode();
				}
			}
			this.factTable=factTable;
			return true;
		}
	}
	
	public Set<String> getJoinTables(ExpressionNode exprNode) throws SQLException{
		Set<String> tableNames=new HashSet<>();
		if(exprNode.isLeafNode()){
			tableNames.addAll(exprNode.getExpr().getFieldsWithTablePrefix().keySet());
		}else{
			tableNames.addAll(getJoinTables(exprNode.getLeftExprNode()));
			tableNames.addAll(getJoinTables(exprNode.getRightExprNode()));
		}
		return tableNames;
	}

	public String getFactTable() {
		return factTable;
	}

	public void setFactTable(String factTable) {
		this.factTable = factTable;
	}
	
}
