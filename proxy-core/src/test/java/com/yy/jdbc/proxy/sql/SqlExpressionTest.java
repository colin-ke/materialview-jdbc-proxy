package com.yy.jdbc.proxy.sql;

import com.yy.jdbc.proxy.sql.parser.SQLParser;
import org.junit.Test;

/**
 * @author colin_ke keqinwu@163.com
 */
public class SqlExpressionTest {

	@Test
	public void rewriteTest() throws Exception {
//		String mtViewSql = "select tt1.a,tt2.b,sum(tt3.c),count(distinct tt4.d),avg(tt4.d) " +
//				"from t1 tt1 join t2 tt2 on tt1.a=tt2.a join t3 tt3 on tt3.a=tt1.a join t4 tt4 on tt4.a=tt1.a " +
//				"where (tt1.b=1 and tt2.b=2)or tt3.d=4 " +
//				"group by tt1.a,tt2.b " +
//				"order by avg(tt4.d) asc,tt2.b desc";
		String mtViewSql = "SELECT time_id,product_id,COUNT(time_id) as ct FROM sales_fact_1997 WHERE product_id>1500  GROUP BY time_id,product_id";
		SqlExpression mtView = SQLParser.genSqlExpression(mtViewSql);
		System.out.println("mt view sql:");
		System.out.println(mtView.toSql());

		System.out.println();
		String sql1 = "SELECT time_id,product_id,COUNT(time_id) FROM sales_fact_1997 WHERE product_id>1500  GROUP BY time_id,product_id";
		String sql2 = "SELECT time_id,product_id,COUNT(time_id) as d FROM sales_fact_1997 WHERE product_id>1500 and time_id=367 GROUP BY time_id,product_id ";
		SqlExpression exp1 = SQLParser.genSqlExpression(sql1);
		SqlExpression exp2 = SQLParser.genSqlExpression(sql2);

		String sql3 = "SELECT time_id,product_id,COUNT(time_id) as d FROM sales_fact_1997 WHERE product_id>1500 and customer_id=100  GROUP BY time_id,product_id";
		SqlExpression exp3 = SQLParser.genSqlExpression(sql3);

//		System.out.println(exp1.tryRewrite(mtView, "mt_table").toSql());
		System.out.println(exp2.tryRewrite(mtView, "mt_table").toSql());
//		System.out.println(exp3.tryRewrite(mtView, "mt_table").toSql());

		System.out.println();
	}

	@Test
	public void joinRewriteTest() throws Exception {
		String viewSql = "SELECT d.month_of_year,f.product_id,COUNT(f.time_id) c \n" +
				"FROM sales_fact_1997 f \n" +
				"JOIN time_by_day d ON f.time_id=d.time_id \n" +
				"WHERE f.product_id>1537  GROUP BY d.month_of_year,f.product_id;";
		SqlExpression mtView = SQLParser.genSqlExpression(viewSql);
		System.out.println(mtView.toSql());

		System.out.println();

		String sql1 = "SELECT d.month_of_year,f.product_id,COUNT(f.time_id) c \n" +
				"FROM sales_fact_1997 f \n" +
				"JOIN time_by_day d ON f.time_id=d.time_id \n" +
				"WHERE f.product_id>2000 GROUP BY d.month_of_year,f.product_id;";
		SqlExpression exp1 = SQLParser.genSqlExpression(sql1);
		System.out.println(exp1.tryRewrite(mtView, "mt_table").toSql());
	}

	@Test
	public void groupByTest() throws Exception {
		String viewSql = "SELECT product_id as pid, customer_id as cid, count(customer_id) as ct " +
				"FROM sales_fact " +
				"WHERE product_id>1569 " +
				"GROUP BY product_id,customer_id;";

		SqlExpression mtView = SQLParser.genSqlExpression(viewSql);
		System.out.println(mtView.toSql());

		System.out.println();

		String sql1 = "SELECT customer_id, count(customer_id) as d " +
				"FROM sales_fact " +
				"WHERE product_id>1569 " +
				"GROUP BY customer_id;";
		SqlExpression exp1 = SQLParser.genSqlExpression(sql1);
		System.out.println(exp1.tryRewrite(mtView, "mt_table").toSql());
	}

	@Test
	public void countDstTest() throws Exception {
		String viewSql = "SELECT time_id,product_id,COUNT(time_id) c ,count(distinct time_id)dc,sum(time_id)sc\n" +
				"FROM sales_fact_1997 WHERE product_id>1537  \n" +
				"GROUP BY time_id,product_id";

		SqlExpression mtView = SQLParser.genSqlExpression(viewSql);
		System.out.println(mtView.toSql());

		System.out.println();

		String sql1 = "SELECT product_id,COUNT(distinct time_id) cs\n" +
				"FROM sales_fact_1997 WHERE product_id>1537 and time_id=728 \n" +
				"GROUP BY product_id;";
		SqlExpression exp1 = SQLParser.genSqlExpression(sql1);
		System.out.println(exp1.tryRewrite(mtView, "mt_table").toSql());
	}



























}
