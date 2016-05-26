package com.yy.jdbc.proxy.sql.parser.select;

import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.sql.*;
import com.yy.jdbc.proxy.sql.OrderBy.Order;
import com.yy.jdbc.proxy.sql.agg.*;
import com.yy.jdbc.proxy.sql.parser.SQL;
import com.yy.jdbc.proxy.sql.where.Condition;
import com.yy.jdbc.proxy.sql.where.logic.And;
import com.yy.jdbc.proxy.sql.where.logic.Or;
import gudusoft.gsqlparser.EAggregateType;
import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.nodes.TCTEList;
import gudusoft.gsqlparser.nodes.TExpression;
import gudusoft.gsqlparser.nodes.TFunctionCall;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class SelectSQL implements SQL {
	public static Log selectSqlLog=LogFactory.getLog(SelectSQL.class);
	private TSelectSqlStatement selectStmt;
	private SelectNode selectNode;
	public SelectSQL() {
	}


	@Override
	public SQLType getSQLType() {
		return SQLType.SELECT;
	}

	public TSelectSqlStatement getSelectStmt() {
		return selectStmt;
	}

	public SqlExpression toSqlExpression() throws SQLException{
		TCTEList tcteList=this.selectStmt.getCteList();
		//拆分成两步，构建with视图的处理树以及select主体处理树
		if (tcteList != null) {
			throw new SQLException("不支持with子句!");
		}
		selectNode = new SelectNode(selectStmt, null, null);
		if (!selectNode.isStarModel()) {
			throw new SQLException("非星型模型，不存在事实表！");
		}
		MaterialView view = new MaterialView();
		view.setFactTable(selectNode.getFactTable());
		Map<String, String> tableReferRealName = getTableRealName(selectNode);
		SqlExpression sqlExpression = buildSqlExpression(selectNode, tableReferRealName);
		buildGroupBy(sqlExpression, selectNode, tableReferRealName);
		buildOrderBy(sqlExpression, selectNode, tableReferRealName);
		if (selectNode.getFilterNode() != null && selectNode.getFilterNode().getWhereExprNode() != null)
			buildCondition(sqlExpression, selectNode, tableReferRealName);
		return sqlExpression;
	}

	@Override
	public Object compile() throws SQLException{
		TCTEList tcteList=this.selectStmt.getCteList();
		//拆分成两步，构建with视图的处理树以及select主体处理树
		if(tcteList!=null){
			throw new SQLException("不支持with子句!");
		}
		SelectNode selectNode=new SelectNode(selectStmt,null,null);
		if(!selectNode.isStarModel()){
			throw new SQLException("非星型模型，不存在事实表！");
		}
		MaterialView view=new MaterialView();
		view.setFactTable(selectNode.getFactTable());
		Map<String,String> tableReferRealName=getTableRealName(selectNode);
		SqlExpression sqlExpression=buildSqlExpression(selectNode,tableReferRealName);
		buildGroupBy(sqlExpression,selectNode,tableReferRealName);
		buildOrderBy(sqlExpression,selectNode,tableReferRealName);
		if(selectNode.getFilterNode()!=null&&selectNode.getFilterNode().getWhereExprNode()!=null)
			buildCondition(sqlExpression,selectNode,tableReferRealName);
		view.setSqlDataStruct(sqlExpression);
		view.setSqlStr(sqlExpression.toSql());
		return view;
	}

	private void buildCondition(SqlExpression sqlExpression,SelectNode selectNode,Map<String,String> tableReferRealName) throws SQLException{
		sqlExpression.withCondition(buildCondition(selectNode.getFilterNode().getWhereExprNode(), tableReferRealName));
	}
	
	private Condition buildCondition(ExpressionNode exprNode,Map<String,String> tableReferRealName) throws SQLException{
		if(!exprNode.isLeafNode()){
			if(exprNode.getLogicAndOr()==EExpressionType.logical_and_t){
				return new And(buildCondition(exprNode.getLeftExprNode(),tableReferRealName),buildCondition(exprNode.getRightExprNode(),tableReferRealName));
			}else{
				return new Or(buildCondition(exprNode.getLeftExprNode(),tableReferRealName),buildCondition(exprNode.getRightExprNode(),tableReferRealName));
			}
		}else{
			return exprNode.getExpr().getValue(tableReferRealName);
		}
	}
	
	private void buildGroupBy(SqlExpression sqlExpression,SelectNode selectNode,Map<String,String> tableReferRealName) throws SQLException{
		GroupByNode groupByNode=selectNode.getGroupByNode();
		if(groupByNode!=null&&groupByNode.getGroupByExpr()!=null){
			List<SQLExpression> groupByExprs=groupByNode.getGroupByExpr();
			List<Field> groupFields=new ArrayList<>();
			for(SQLExpression expr:groupByExprs){
				Entry<String, String> fieldWithAliasPrefix=SQLUtil.getTableNameAndFieldName(expr.getExpr());
				Field field=new Field(tableReferRealName.get(fieldWithAliasPrefix.getKey()),fieldWithAliasPrefix.getValue());
				groupFields.add(field);
			}
			sqlExpression.withGroupBy(groupFields.toArray(new Field[groupFields.size()]));
		}
		
	}
	private void buildOrderBy(SqlExpression sqlExpression,SelectNode selectNode,Map<String,String> tableReferRealName) throws SQLException{
		OrderByNode orderByNode=selectNode.getOrderByNode();
		if(orderByNode!=null&&orderByNode.getColAndOrderByType()!=null){
			List<Pair<SQLExpression,String>> orderByExprs=orderByNode.getColAndOrderByType();
			int size=orderByExprs.size();
			OrderBy[] orderBys= new OrderBy[size];
			for(int i=0;i<size;i++){
				TExpression expr=orderByExprs.get(i).getKey().getExpr();
				if(SQLExpression.isSimpleObjectNameExpr(expr)){
					Entry<String, String> fieldWithAliasPrefix=SQLUtil.getTableNameAndFieldName(expr);
					Field field=new Field(tableReferRealName.get(fieldWithAliasPrefix.getKey()),fieldWithAliasPrefix.getValue());
					field.withAlias(selectNode.getProjectionNode().getColumnReferHintAlias().get(expr.toString()));
					orderBys[i]=new OrderBy(field,Order.valueOf(orderByExprs.get(i).getValue()));
				}else if(SQLExpression.isFuncCallExpr(expr)){
					String alias=selectNode.getProjectionNode().getColumnReferHintAlias().get(expr.toString());
					Selectable field=buildFunc(expr,tableReferRealName);
					((Aggregation)field).withAlias(alias);
					orderBys[i]=new OrderBy(field,Order.valueOf(orderByExprs.get(i).getValue()));
				}else{
					throw new SQLException("order字段不支持"+expr);
				}
				
			}
			sqlExpression.withOrderBy(orderBys);
		}
		
	}
	
	private SqlExpression buildSqlExpression(SelectNode selectNode,Map<String,String> tableReferRealName) throws SQLException{
		List<Selectable> fields=new ArrayList<>();
		List<SQLExpression> fieldExprs=selectNode.getProjectionNode().getSelectColumnsExpr();

		for(SQLExpression sqlExpr:fieldExprs){
			if(SQLExpression.isSimpleObjectNameExpr(sqlExpr.getExpr())){
				Entry<String, String> fieldWithAliasPrefix=SQLUtil.getTableNameAndFieldName(sqlExpr.getExpr());
				Field field=new Field(tableReferRealName.get(fieldWithAliasPrefix.getKey()),fieldWithAliasPrefix.getValue());
				field.withAlias(selectNode.getProjectionNode().getColumnReferHintAlias().get(sqlExpr.getExpr().toString()));
				fields.add(field);
			}else if(SQLExpression.isFuncCallExpr(sqlExpr.getExpr())){
				String alias=selectNode.getProjectionNode().getColumnReferHintAlias().get(sqlExpr.getExpr().toString());
				Selectable field=buildFunc(sqlExpr.getExpr(),tableReferRealName);
				((Aggregation)field).withAlias(alias);
				fields.add(field);
			}else{
				throw new SQLException("select字段不支持"+sqlExpr);
			}
		}
		List<String> onConditions=new ArrayList<>();
		JoinNode joinNode=selectNode.getJoinNode();
		//field=field这个没有抽象出来，直接特殊处理
		while(joinNode!=null){
			TExpression expr=joinNode.getOnConditionNode().getExpr().getExpr();
			String opt=joinNode.getOnConditionNode().getExpr().getExpr().getOperatorToken().toString();
			if(!"=".equals(opt)){
				throw new SQLException("on条件不支持："+opt);
			}
			if(!SQLExpression.isSimpleObjectNameExpr(expr.getLeftOperand())||!SQLExpression.isSimpleObjectNameExpr(expr.getRightOperand())){
				throw new SQLException("on条件不支持函数");
			}
			Entry<String, String> leftEntry=SQLUtil.getTableNameAndFieldName(expr.getLeftOperand());
			Entry<String, String> rightEntry=SQLUtil.getTableNameAndFieldName(expr.getRightOperand());
			String condition=tableReferRealName.get(leftEntry.getKey())+"."+leftEntry.getValue()+"="+tableReferRealName.get(rightEntry.getKey())+"."+rightEntry.getValue();
			onConditions.add(condition);
			joinNode=joinNode.getChildNode();
		}
		List<String> tableNames=getTableNames(selectNode);
		From from=null;
		if(onConditions.isEmpty()){
			from=new From(tableNames.toArray(new String[tableNames.size()]),null);
		}else{
			from=new From(tableNames.toArray(new String[tableNames.size()]),onConditions.toArray(new String[tableNames.size()]));				
		}
		return new SqlExpression(fields,from);
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
	private List<String> getTableNames(SelectNode selectNode){
		List<String> result=new ArrayList<>();
		if(!selectNode.hasJoin()){
			result.add(selectNode.getFromNode().getTableName());
		}else{
			JoinNode joinNode=selectNode.getJoinNode();
			result.add(joinNode.getLeftTable().getTableName());
			result.add(joinNode.getRightTable().getTableName());
			joinNode=joinNode.getChildNode();
			while(joinNode!=null){
				result.add(joinNode.getRightTable().getTableName());
				joinNode=joinNode.getChildNode();
			}
		}
		return result;
	}
	private Selectable buildFunc(TExpression funcExpr,Map<String,String> tableAliasWithName) throws SQLException {
		if(SQLExpression.isSimpleObjectNameExpr(funcExpr)){
			Entry<String, String> fieldWithAliasPrefix=SQLUtil.getTableNameAndFieldName(funcExpr);
			Field field=new Field(tableAliasWithName.get(fieldWithAliasPrefix.getKey()),fieldWithAliasPrefix.getValue());
			return field;
		}else if (SQLExpression.isFuncCallExpr(funcExpr)) {
			TFunctionCall functionCall=funcExpr.getFunctionCall();
			EAggregateType type=functionCall.getAggregateType();
			if(type==EAggregateType.distinct){
				if(functionCall.getFunctionName().toString().equalsIgnoreCase("count")){
					if(functionCall.getArgs().getExpression(0).getLeftOperand()!=null)
						return buildAggFunc("count_distinct",buildFunc(functionCall.getArgs().getExpression(0).getLeftOperand(), tableAliasWithName));
					else
						return buildAggFunc("count_distinct",buildFunc(functionCall.getArgs().getExpression(0), tableAliasWithName));
				}else{
					throw new SQLException("不支持"+functionCall.getFunctionName()+"(distinct())");
				}
				
			}else if(type==EAggregateType.unique||type==EAggregateType.all){
				throw new SQLException("不支持汇聚函数:"+type.name());
			}else{
				return buildAggFunc(functionCall.getFunctionName().toString(),buildFunc(functionCall.getArgs().getExpression(0),tableAliasWithName));
			}
		}else{
			throw new SQLException("select字段不支持表达式:"+funcExpr);
		}
	}
	
	private Aggregation buildAggFunc(String funcName,Selectable target) throws SQLException{
		switch (funcName.toLowerCase()) {
		case "avg":
			return new Avg(target);
		case "count":
			return new Count(target);
		case "max":
			return new Max(target);
		case "min":
			return new Min(target);
		case "sum":
			return new Sum(target);
		case "count_distinct":
			return new DistinctCount(target);
		default:
			throw new SQLException("不支持函数:"+funcName);
		}
	}
	
	private void addTableAliasAndName(FromNode fromNode,Map<String, String> map){
		map.put(fromNode.getAlias(),fromNode.getTableName());
	}
	@Override
	public TCustomSqlStatement getSQLStmt() {
		return this.selectStmt;
	}

	@Override
	public void setSQLStmt(TCustomSqlStatement sqlStatement) {
		this.selectStmt=(TSelectSqlStatement)sqlStatement;
	}
	
	public boolean hasHaving(){
		if(this.selectStmt.getGroupByClause()!=null){	
			if(this.selectStmt.getGroupByClause().getHavingClause()!=null){
				return true;
			}
		}
		return false;
	}
}
