package com.yy.jdbc.proxy.sql.parser.select;

import gudusoft.gsqlparser.EJoinType;
import gudusoft.gsqlparser.nodes.TJoin;
import gudusoft.gsqlparser.nodes.TJoinItem;
import gudusoft.gsqlparser.nodes.TTable;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Multimap;

public class SelectNode implements TreeNode {
	private TreeNode parentNode;
	private JoinNode joinNode;
	private ProjectionNode projectionNode;
	private FromNode fromNode;
	private FilterNode filterNode;
	private GroupByNode groupByNode;
	private OrderByNode orderByNode;
	
	private TSelectSqlStatement sqlStmt;
	//查询别名
	private String alias;
	
	//每个表对应的字段和字段的别名
	private Map<String, String> tableFieldNameWithAlias;
	
	public SelectNode(TSelectSqlStatement sqlStmt,String alias,TreeNode parentNode) throws SQLException{
		this.sqlStmt=sqlStmt;
		this.alias=alias;
		this.tableFieldNameWithAlias=new HashMap<String,String>();
		this.parentNode=parentNode;
		init();
	}
	
	//只能是joinNode分解出来的节点
	public SelectNode(ProjectionNode projectionNode,FromNode fromNode,
			FilterNode filterNode,String alias,
			Map<String, String> withSubqueryAliasOfColumn,
			Multimap<String, SQLExpression> extraFieldsWhichNotInSelect) throws SQLException {
		this.projectionNode=projectionNode;
		this.fromNode=fromNode;
		this.filterNode=filterNode;
		this.tableFieldNameWithAlias=new HashMap<String,String>();
		this.alias=alias;
		this.init();
	}

	
	@Override
	public void init() throws SQLException {
		if(sqlStmt.isCombinedQuery())
			throw new SQLException(String.format("不支持union等sql操作"));
		else if(sqlStmt.joins.getJoin(0).getJoinItems().size()>=1)
			//显示指定join的joinNode节点
			buildJoinNode();
		else if(sqlStmt.tables.size()>1)
			//from后面带多个表，作为joinNode节点，join类型为inner
//			buildImplicitJoinNode();
			throw new SQLException("多表关联的sql只支持join操作");
		else 
			//简单的select sql生成的SelectNode节点
			buildBaseNode();
	}
	
	
				
				
	private void buildJoinNode() throws SQLException{
		buildBaseNode();
		TJoin join=sqlStmt.joins.getJoin(0);
		int joinItemSize=join.getJoinItems().size();
		
		List<TTable> joinTables=new ArrayList<>(joinItemSize+1);
		List<EJoinType> joinType=new ArrayList<>(joinItemSize);
		List<ExpressionNode> joinOnExprNodes=new ArrayList<>(joinItemSize);
		joinTables.add(join.getTable());
		
		for(int i=0;i<joinItemSize;i++){
			TJoinItem joinItem=join.getJoinItems().getJoinItem(i);
			joinTables.add(joinItem.getTable());
			joinType.add(joinItem.getJoinType());
			joinOnExprNodes.add(ExpressionNode.getExpressionsFromExpr(joinItem.getOnCondition(),null));
		}
		buildRootJoinNode(joinTables, joinType, joinOnExprNodes);
	}
	
//	private void buildImplicitJoinNode() throws SQLException{
//		buildBaseNode();
//		int tableSize=sqlStmt.tables.size();
//		int joinItemSize=tableSize-1;
//		List<TTable>  joinTables=new ArrayList<>(tableSize);
//		List<EJoinType> joinType=new ArrayList<>(joinItemSize);
//		TTableList tableList=sqlStmt.tables;
//		for(int i=0;i<joinItemSize;i++){
//			joinTables.add(tableList.getTable(i));
//			joinType.add(EJoinType.inner);
//		}
//		this.joinNode=buildRootJoinNode(joinTables, joinType,null);
//	}
				
	
	private void buildBaseNode() throws SQLException{
		this.projectionNode=new ProjectionNode(sqlStmt);
		this.fromNode=new FromNode(sqlStmt.tables.getTable(0),this);
		this.filterNode=new FilterNode(sqlStmt);
		this.groupByNode=new GroupByNode(sqlStmt);
		if(sqlStmt.getOrderbyClause()!=null){
			this.orderByNode=new OrderByNode(sqlStmt);
		}
	}
	
	private void buildRootJoinNode(List<TTable> joinTables,List<EJoinType> joinTypes,List<ExpressionNode> joinOnExprNodes) throws SQLException{
		FromNode leftTableFromNode=new FromNode(joinTables.get(0),this);
		FromNode rightTableFromNode=new FromNode(joinTables.get(1),this);
		
		//join根节点
		JoinNode node=new JoinNode(joinTypes.get(0), leftTableFromNode,
				rightTableFromNode,joinOnExprNodes.get(0),null);
		this.joinNode=node;
		
		for(int index=1;index<joinTypes.size();index++){
			rightTableFromNode=new FromNode(joinTables.get(index+1),this);
			JoinNode tmpJoinNode=new JoinNode(joinTypes.get(index), 
					node,rightTableFromNode,joinOnExprNodes.get(index));
			node.setChildNode(tmpJoinNode);
			node=tmpJoinNode;
		}
	}
	

	public FromNode getFromNode() {
		return fromNode;
	}


	public ProjectionNode getProjectionNode() {
		return projectionNode;
	}


	public FilterNode getFilterNode() {
		return filterNode;
	}

	public TSelectSqlStatement getSqlStmt() {
		return sqlStmt;
	}


	public JoinNode getJoinNode() {
		return joinNode;
	}

	public Map<String, String> getFieldNameWithAlias() {
		return tableFieldNameWithAlias;
	}
	public void setFieldNameWithAlias(Map<String, String> tableFieldNameWithAlias) {
		this.tableFieldNameWithAlias=tableFieldNameWithAlias;
	}

	public boolean isStarModel() throws SQLException{
		return this.joinNode==null||this.joinNode.isStartModel();
	}
	
	public String getFactTable() throws SQLException{
		if(joinNode!=null){
			return getTableRealName(this).get(this.joinNode.getFactTable()); 
		}else{
			return this.fromNode.getTableName();
		}
	}
	private Map<String,String> getTableRealName(SelectNode selectNode){
		Map<String,String> result=new HashMap<>();
		if(!selectNode.hasJoin()){
			addTableAliasAndName(selectNode.getFromNode(), result);
		}else{
			JoinNode joinNode=selectNode.getJoinNode();
			addTableAliasAndName(joinNode.getLeftTable(),result);
			addTableAliasAndName(joinNode.getRightTable(),result);
			joinNode=joinNode.getChildNode();
			while(joinNode!=null){
				addTableAliasAndName(joinNode.getRightTable(), result);
				joinNode=joinNode.getChildNode();
			}
		}
		return result;
	}
	
	private void addTableAliasAndName(FromNode fromNode,Map<String, String> map){
		map.put(fromNode.getAlias(),fromNode.getTableName());
	}
	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}
	
	public GroupByNode getGroupByNode() {
		return groupByNode;
	}

	public OrderByNode getOrderByNode() {
		return orderByNode;
	}

	public TreeNode getParentNode() {
		return parentNode;
	}

	public void setParentNode(TreeNode parentNode) {
		this.parentNode = parentNode;
	}
	
	public boolean hasJoin(){
		return this.joinNode!=null;
	}
}
