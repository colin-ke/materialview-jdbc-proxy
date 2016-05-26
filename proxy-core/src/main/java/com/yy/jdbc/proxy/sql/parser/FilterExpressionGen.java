package com.yy.jdbc.proxy.sql.parser;


/**
 * 根据不同条件标识符生成条件表达式，默认实现是 left op right,如 name > 'csm'
 * 
 * @author estan estan@yy.com
 * 
 */
public interface FilterExpressionGen {
	String gen(String leftRight, String rightValue, boolean bracket);

	public static class SimpleFilterExpressionGen implements
			FilterExpressionGen {
		private String op;

		public SimpleFilterExpressionGen(String op) {
			this.op = op;
		}

		@Override
		public String gen(String leftRight, String rightValue, boolean bracket) {
			String b = "";
			if (bracket) {
				b = "'";
			}
			return leftRight + " " + op.toString() + " " + b +rightValue+ b;
		}

	}
}
