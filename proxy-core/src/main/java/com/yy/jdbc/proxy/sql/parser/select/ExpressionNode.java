package com.yy.jdbc.proxy.sql.parser.select;

import gudusoft.gsqlparser.EExpressionType;
import gudusoft.gsqlparser.nodes.TExpression;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ExpressionNode{
	private static final List<EExpressionType> supportOperation;
	
	static{
		supportOperation=new ArrayList<>();
		supportOperation.add(EExpressionType.logical_and_t);
		supportOperation.add(EExpressionType.logical_or_t);
		supportOperation.add(EExpressionType.between_t);
		supportOperation.add(EExpressionType.parenthesis_t);
		supportOperation.add(EExpressionType.simple_comparison_t);
		supportOperation.add(EExpressionType.simple_object_name_t);
		supportOperation.add(EExpressionType.in_t);
		supportOperation.add(EExpressionType.function_t);
		supportOperation.add(EExpressionType.function_t);
		supportOperation.add(EExpressionType.case_t);
		supportOperation.add(EExpressionType.simple_constant_t);
	}
	
	private SQLExpression expr;
	private EExpressionType logicAndOr;
	private ExpressionNode leftExprNode;
	private ExpressionNode rightExprNode;
	
	public ExpressionNode(ExpressionNode leftExprNode,
			ExpressionNode rightExprNode,EExpressionType logicAndOr,SQLExpression expr){
		this.logicAndOr=logicAndOr;
		this.leftExprNode=leftExprNode;
		this.rightExprNode=rightExprNode;
		this.expr=expr;
	}
	
	public ExpressionNode getLeftExprNode() {
		return leftExprNode;
	}
	public ExpressionNode getRightExprNode() {
		return rightExprNode;
	}

	public void setExpr(SQLExpression expr) {
		this.expr = expr;
	}

	public EExpressionType getLogicAndOr() {
		return logicAndOr;
	}
	
	public SQLExpression getExpr() {
		return expr;
	}
	
	public boolean isLeafNode(){
		return this.leftExprNode==null&&this.rightExprNode==null;
	}
	
	public void setLogicAndOr(EExpressionType logicAndOr) {
		this.logicAndOr = logicAndOr;
	}

	public void setLeftExprNode(ExpressionNode leftExprNode) {
		this.leftExprNode = leftExprNode;
	}

	public void setRightExprNode(ExpressionNode rightExprNode) {
		this.rightExprNode = rightExprNode;
	}

	
	public static boolean isSupportOperator(EExpressionType type){
		return supportOperation.contains(type);
	}
	
	public static ExpressionNode getExpressionsFromExpr(TExpression expr,String tableName) throws SQLException{
		return buildExprNode(expr,tableName);
	}
	
	
	public static ExpressionNode buildExprNode(TExpression expr,String tableName) throws SQLException{
		if(expr==null)return null;
		if(!SQLExpression.isAndExpr(expr)
				&&!SQLExpression.isOrExpr(expr)){
			if(SQLExpression.isParenthesisExpr(expr)){
				return buildExprNode(expr.getLeftOperand(),tableName);
			}else{
				SQLExpression sqlExpr=SQLExpression.getExpressionInstance(expr);
				sqlExpr.formatField(tableName);
				return new ExpressionNode(null,null,null,sqlExpr);
			}
		}
		
		ExpressionNode leftNode=buildExprNode(expr.getLeftOperand(),tableName);
		ExpressionNode rightNode=buildExprNode(expr.getRightOperand(),tableName);
		ExpressionNode exprNode=new ExpressionNode(leftNode,rightNode,expr.getExpressionType(),null);
		return exprNode;
	}
	
}
