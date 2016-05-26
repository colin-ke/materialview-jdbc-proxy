package com.yy.jdbc.proxy.sql.parser;


/**
 * 条件表达式中能够支持的操作符
 * 
 * @author estan
 * 
 */
public enum FilterOp {

	EQ("=", new FilterExpressionGen.SimpleFilterExpressionGen("=")),
	// TODO 需要根据不同数据库方言来生成
	NE("!=", new FilterExpressionGen.SimpleFilterExpressionGen("!=")),
	//
	GT(">", new FilterExpressionGen.SimpleFilterExpressionGen(">")), //
	GE(">=", new FilterExpressionGen.SimpleFilterExpressionGen(">=")), //
	LE("<=", new FilterExpressionGen.SimpleFilterExpressionGen("<=")), //
	LT("<", new FilterExpressionGen.SimpleFilterExpressionGen("<")), //
	LGTE(">=,<=", new FilterExpressionGen() {

		@Override
		public String gen(String leftRight, String rightValue, boolean bracket) {
			String[] v = rightValue.split(",");
			String b = "";
			if (bracket) {
				b = "'";
			}
			if (v.length != 2) {
				throw new RuntimeException("the value " + rightValue
						+ " error!");
			}
			return leftRight + " >= " + b + v[0] + b + " and " + leftRight
					+ " <= " + b +v[1] + b;
		}
	}), IN("in", new FilterExpressionGen() {

		@Override
		public String gen(String leftRight, String rightValue, boolean bracket) {
			String b = "";
			if (bracket) {
				b = "'";
			}
			String[] v = rightValue.split(",");
			StringBuilder inSeg = new StringBuilder();
			for (int i = 0; i < v.length; i++) {
				inSeg.append(b).append(v[i]).append(b);
				if (i != v.length - 1) {
					inSeg.append(",");
				}
			}
			return leftRight + " in (" + inSeg + " )";
		}
	})//
	,
	NOTIN("notin", new FilterExpressionGen() {

		@Override
		public String gen(String leftRight, String rightValue, boolean bracket) {
			String b = "";
			if (bracket) {
				b = "'";
			}
			String[] v = rightValue.split(",");
			StringBuilder inSeg = new StringBuilder();
			for (int i = 0; i < v.length; i++) {
				inSeg.append(b).append(v[i]).append(b);
				if (i != v.length - 1) {
					inSeg.append(",");
				}
			}
			return leftRight + " not in (" + inSeg + " )";
		}
	})
	;
	private String opString;
	private FilterExpressionGen gen;

	FilterOp(String opString, FilterExpressionGen gen) {
		this.opString = opString;
		this.gen = gen;
	}

	public String getOpString() {
		return opString;
	}

	public String genExpression(String leftRight, String rightValue,
			boolean bracket) {
		return gen.gen(leftRight, rightValue, bracket);
	}
}

