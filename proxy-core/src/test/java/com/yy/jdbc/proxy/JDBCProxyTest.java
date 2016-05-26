package com.yy.jdbc.proxy;

import com.yy.jdbc.proxy.util.DbConnectionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 物化视图整体测试
 *
 * @author ZhangXueJun
 */
public class JDBCProxyTest {

    // 数据库连接
    private Connection conn;

    private String viewName = "mv_view_zhangxuejun_02";
    private boolean drop = false;

    @Before
    public void setUp() throws SQLException {
        conn = DbConnectionManager.getProxyConnection();
        StringBuilder builder = new StringBuilder();
//        builder.append(" create materialized view " + viewName + " build immediate refresh complete ENABLE QUERY REWRITE as ").append("\n");
//        builder.append(" select game_name,sum(game_id) as cnt from dim_game group by game_name order by game_name desc; ").append("\n");
        builder.append(" create materialized view " + viewName + " as  ");
        builder.append("SELECT month_of_year,product_id,COUNT(f.time_id)  \n" +
                "FROM sales_fact_1997 f \n" +
                "JOIN time_by_day d ON f.time_id=d.time_id \n" +
                "WHERE product_id>1537  \n" +
                "GROUP BY month_of_year,product_id");
        createMaterialView(builder.toString());
    }

    @After
    public void close() throws SQLException {
        if (drop) {
            dropMaterialView();
        }
        DbConnectionManager.closeConnection(conn);
    }

    private void createMaterialView(String sql) throws SQLException {
        conn.createStatement().execute(sql);
    }

    private void dropMaterialView() throws SQLException {
        conn.createStatement().execute("drop materialized view " + viewName);
    }

    @Test
    public void test01() throws SQLException {
        String sql = "SELECT month_of_year,product_id,COUNT(f.time_id)  \n" +
                "FROM sales_fact_1997 f \n" +
                "JOIN time_by_day d ON f.time_id=d.time_id \n" +
                "WHERE product_id>1537  \n" +
                "GROUP BY month_of_year,product_id";
        conn.createStatement().execute(sql);
    }
}