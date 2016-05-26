package com.yy.jdbc.proxy.mview;

import com.yy.jdbc.proxy.mview.common.BaseDAO;
import com.yy.jdbc.proxy.mview.manager.MaterialViewProvider;
import com.yy.jdbc.proxy.sql.Field;
import com.yy.jdbc.proxy.sql.OrderBy;
import com.yy.jdbc.proxy.sql.SqlExpression;
import com.yy.jdbc.proxy.util.DbConnectionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;

/**
 * @author ZhangXueJun
 */
public class MaterialViewTest extends BaseDAO {

    private MaterialViewProvider service = new MaterialViewProvider();

    private Connection conn;

    @Before
    public void setUp() {
        conn = DbConnectionManager.getConnection();
    }

    @After
    public void close() {
        DbConnectionManager.closeConnection(conn);
    }

    @Test
    public void isExists() throws SQLException {
        boolean result = service.isExists(conn, "mv_test0");
        System.out.println(result);
    }

    @Test
    public void create() throws SQLException {
        MaterialView materialView = new MaterialView();
        materialView.setViewName("mv_test_zhangxuejun_06");
        materialView.setSqlStr("select * from object_privilege");
        materialView.setRewrite(true);
        materialView.setRefresh(MaterialView.Refresh.complete);
        materialView.setBuild(MaterialView.Build.immediate);
        materialView.setSqlDataStruct(new SqlExpression(null, null).withOrderBy(OrderBy.asc(new Field("admin_user", "user_id"))));
        materialView.setFactTable("object_privilege");
        materialView.setCreateSQL("create materialview xxx ");
        boolean result = service.create(conn, materialView);
    }

    @Test
    public void testComplexCreate() throws SQLException {
        MaterialView materialView = new MaterialView();
        materialView.setViewName("mv_test04");
        materialView.setSqlStr("select a.user_id, a.user_name, a.passport, a.`status`, a.create_user, a.create_time,\n" +
                "a.update_user, a.update_time, a.is_apply_security, a.admin, a.manage_users, b.user_role_id, b.role_id,\n" +
                "c.role_name\n" +
                "from admin_user a\n" +
                "join admin_user_role b on a.user_id = b.user_id\n" +
                "join admin_role c on c.role_id = b.role_id");
        materialView.setRewrite(true);
        materialView.setRefresh(MaterialView.Refresh.complete);
        materialView.setBuild(MaterialView.Build.immediate);
        materialView.setSqlDataStruct(new SqlExpression(null, null).withOrderBy(OrderBy.asc(new Field("admin_user", "user_id"))));
        materialView.setFactTable("admin_user");
        materialView.setCreateSQL("create materialview xxx ");
        boolean result = service.create(conn, materialView);
    }

    @Test
    public void list() throws SQLException {
        Collection<MaterialView> results = service.listAll(conn);
        System.out.println(results.size());
    }

    @Test
    public void get() throws SQLException {
        MaterialView mv = service.get(conn, "mv_test03");
        System.out.println();
    }

    @Test
    public void delete() throws SQLException {
        boolean result = service.delete(conn, "mv_test01");
    }

    @Test
    public void update() throws SQLException {
        MaterialView materialView = new MaterialView();
        materialView.setViewName("mv_test01");
        materialView.setSqlStr("select a.user_id, a.user_name, a.passport, a.`status`, a.create_user, a.create_time,\n" +
                "a.update_user, a.update_time, a.is_apply_security, a.admin, a.manage_users, b.user_role_id, b.role_id,\n" +
                "c.role_name\n" +
                "from admin_user a\n" +
                "join admin_user_role b on a.user_id = b.user_id\n" +
                "join admin_role c on c.role_id = b.role_id");
        materialView.setRewrite(true);
        materialView.setRefresh(MaterialView.Refresh.complete);
        materialView.setBuild(MaterialView.Build.deferred);
        materialView.setSqlDataStruct(new SqlExpression(null, null).withOrderBy(OrderBy.asc(new Field("admin_user", "user_id"))));
        materialView.setFactTable("admin_user");
        materialView.setCreateSQL("create materialview xxx ");
        materialView.setStore(true);
        boolean result = service.update(conn, materialView);
    }

    @Test
    public void getMaterialViewListByFactTable() throws SQLException {
        Collection<MaterialView> results = service.getMaterialViewListByFactTable(conn, "admin_privilege");
        System.out.println(results.size());
    }

    @Test
    public void testBatchAdd() throws SQLException {
        Object params[][] = new Object[2][2];
        for(int i=0;i<params.length;i++){  //3
            params[i] = new Object[]{i+1,i + "@sina.com"};
        }
        runner.batch(conn, "insert into mv_test04_admin_role(role_id,role_name) values( ?,?)", params);
        System.out.println();
    }

    @Test
    public void refresh() throws SQLException {
        service.refresh(conn, "mv_test01");
    }

    @Test
    public void testCreateIndex() throws SQLException {
        String sql = "CREATE INDEX material_view_index ON material_view (viewName,build);";
        runner.update(conn, sql);
    }
    @Test
    public void dropTable() throws SQLException {
        String tableName = "material_view";
        int a = runner.update(conn, "drop table if exists " + tableName);
        System.out.println();
    }

    @Test
    public void showMaterialViews() throws SQLException {
        ResultSet rs = service.showMaterialViews(conn);

        if (rs != null) {
            ResultSetMetaData metaData = rs.getMetaData();
            int cols = metaData.getColumnCount();
            for (int i = 1; i <= cols; i++) {
                logger.info("show mviews");
                String columnName = metaData.getColumnName(i);
                while (rs.next()) {
                    logger.info(rs.getString(columnName));
                }
            }
        }
    }


    @Test
    public void showCreateMaterialView() throws SQLException {
        String viewName = "mv_test01";
        ResultSet rs = service.showCreateMaterialView(conn, "mv_test01");
        if (rs != null) {
            ResultSetMetaData metaData = rs.getMetaData();
            int cols = metaData.getColumnCount();
            for (int i = 1; i <= cols; i++) {
                logger.info("show create mview " + viewName);
                String columnName = metaData.getColumnName(i);
                while (rs.next()) {
                    logger.info(rs.getString(columnName));
                }
            }
        }
    }
}
