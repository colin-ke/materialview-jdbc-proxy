package com.yy.jdbc.proxy.dirver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.DatabaseMetaData;
import com.yy.jdbc.proxy.sql.SqlExpression;
import com.yy.jdbc.proxy.sql.parser.SQLParser;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DriverTest {
	private Connection conn;

	@Before
	public void setUp() throws SQLException {
		conn = DriverManager
				.getConnection("jdbc:proxy_mysql://183.61.12.83:3306/jcl?"
						+ "user=metadata&password=vkyKSdqYVlT7C3g6CVIF&useUnicode=true&characterEncoding=UTF8");
	
    }

	@After
	public void close() throws SQLException {
		conn.close();
	}

	@Test
	public void testJoin() throws SQLException {

//		System.out.println(conn.prepareStatement("select * from object_privilege").executeQuery().getFetchSize());;
//		String sql = "select tt1.a,tt2.b,tt3.c,tt4.d from t1 tt1 join t2 tt2 on tt1.a=tt2.a join t3 tt3 on tt3.a=tt1.a join t4 tt4 on tt4.a=tt1.a where (tt1.b=1 and tt2.b=2)or tt3.d=4 group by tt1.c,tt2.c order by tt3.c asc,tt4.c desc";
//		conn.prepareStatement("show tables;");
//		conn.createStatement().executeQuery(sql);
//		conn.createStatement().execute("create materialized view mv_sales_fact_month1 as SELECT month_of_year,f.product_id,COUNT(f.time_id)  FROM sales_fact_1997 f JOIN time_by_day d ON f.time_id=d.time_id WHERE d.product_id>1537  GROUP BY month_of_year,d.product_id;");
//		conn.createStatement().execute("drop materialized view m_view111");
//		conn.createStatement().executeQuery("select game_name,sum(game_id) as cnt from dim_game group by game_name order by game_name desc");
//		conn.createStatement().execute("set auto_rewrite=on;");
//		conn.createStatement().executeQuery("show tables;");
		conn.createStatement().execute("set auto_rewrite=on");
		conn.createStatement().executeQuery("SELECT d.month_of_year,f.product_id,COUNT(f.time_id) c FROM sales_fact_1997 f JOIN time_by_day d ON f.time_id=d.time_id WHERE f.product_id>1537  GROUP BY d.month_of_year,f.product_id;");
	}

	@Test
	public void test() throws SQLException {
		String sql = "select aa,bb,cc,count(distinct(sum(ddt))) from tttt where (a=1 and c=2) or c>10 and kk<=100 and mk in (1,2,3) group by mmk order by aaaaaaa asc;";
		conn.createStatement().execute("show tables;");
		conn.createStatement().executeQuery(sql);
	}

	@Test
	public void testToSql() throws Exception {
		String sql = "select tt1.a,tt2.b,tt3.c,tt4.d " +
				"from t1 tt1 join t2 tt2 on tt1.a=tt2.a join t3 tt3 on tt3.a=tt1.a join t4 tt4 on tt4.a=tt1.a " +
				"where (tt1.b=1 and tt2.b=2)or tt3.d=4 " +
				"group by tt1.c,tt2.c " +
				"order by tt3.c asc,tt4.c desc";
		SqlExpression sqlExp = SQLParser.genSqlExpression(sql);
		System.out.println(sqlExp.toSql());
	}

	@Test
	public void testRewrite() throws Exception {
//		String mtViewSql = "select tt1.a,tt2.b,sum(tt3.c),count(distinct tt4.d),avg(tt4.d) " +
//				"from t1 tt1 join t2 tt2 on tt1.a=tt2.a join t3 tt3 on tt3.a=tt1.a join t4 tt4 on tt4.a=tt1.a " +
//				"where (tt1.b=1 and tt2.b=2)or tt3.d=4 " +
//				"group by tt1.a,tt2.b " +
//				"order by avg(tt4.d) asc,tt2.b desc";
		String mtViewSql = "SELECT time_id,product_id,COUNT(time_id) FROM sales_fact_1997 WHERE product_id>1500  GROUP BY time_id,product_id";
		SqlExpression mtView = SQLParser.genSqlExpression(mtViewSql);
		System.out.println("mt view sql:");
		System.out.println(mtView.toSql());

		String sql1 = "SELECT time_id,product_id,COUNT(time_id) FROM sales_fact_1997 WHERE product_id>1500  GROUP BY time_id,product_id";
		SqlExpression exp1 = SQLParser.genSqlExpression(sql1);

		System.out.println(exp1.tryRewrite(mtView, "mt_table").toSql());
	}
}
