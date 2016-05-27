package com.yy.jdbc.proxy.mview.manager;

import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.mview.common.BaseDAO;
import com.yy.jdbc.proxy.mview.common.manager.IMaterialViewRepository;
import com.yy.jdbc.proxy.sql.SqlExpression;
import com.yy.jdbc.proxy.util.SerializeHelper;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;

/**
 * 物化视图JDBC操作
 *
 * @author ZhangXueJun
 */
public class MaterialViewRepository
        extends BaseDAO
        implements IMaterialViewRepository {

    // 表名
    public static final String TABLE_NAME = "material_view";

    // 汇总数据量查询语句
    private static final String countSQL = "select count(*) countNum from " + TABLE_NAME;

    // 获取所有查询语句
    private static final String selectSQL = "select * from " + TABLE_NAME;

    // 指定删除语句
    private static final String deleteSQL = "delete from " + TABLE_NAME + " where viewName=?;";

    // 插入语句
    private static final String insertSQL =
            "INSERT INTO `" + TABLE_NAME + "` (`viewName`, `build`, `refresh`, `rewrite`, `sqlStr`, `factTable`, `sqlDataStruct`, `store`, `createSQL`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    // 更新语句
    private static final String updateSQL =
            "UPDATE `" + TABLE_NAME + "` SET `build`=?, `refresh`=?, `rewrite`=?, `sqlStr`=?, `factTable`=?, `sqlDataStruct`=?, `store`=?, `createSQL`=? WHERE `viewName`=?;";


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isExists(Statement statement, String viewName) throws SQLException {
        String sql = countSQL + " where viewName=?;";
        boolean result = runner.query(statement.getConnection(), sql, new ResultSetHandler<Boolean>() {
            @Override
            public Boolean handle(ResultSet rs) throws SQLException {
                int countNum = 0;
                while (rs.next()) {
                    countNum = rs.getInt("countNum");
                }
                return countNum != 0;
            }
        }, new Object[]{viewName});
        return result;
    }

    /**
     * {@inheritDoc}
     * @param statement
     * @param view
     */
    @Override
    public boolean create(Statement statement, MaterialView view) throws SQLException {
        runner.insert(
                statement.getConnection(), insertSQL,
                new MaterialViewResultSetEmptyHandler(),
                new Object[]
                        {
                                view.getViewName(), view.getBuild().name(),
                                view.getRefresh().name(), view.isRewrite(),
                                view.getSqlStr(), view.getFactTable(),
                                SerializeHelper.serialize(view.getSqlDataStruct()),
                                view.isStore(), view.getCreateSQL()
                        }
        );
        return true;
    }

    //    @Override
    public boolean update(Statement statement, MaterialView view) throws SQLException {
        runner.update(statement.getConnection(), updateSQL, new Object[]
                {
                        view.getBuild().name(), view.getRefresh().name(),
                        view.isRewrite(), view.getSqlStr(),
                        view.getFactTable(), SerializeHelper.serialize(view.getSqlDataStruct()),
                        view.isStore(), view.getCreateSQL(), view.getViewName()
                });
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete(Statement statement, String viewId) throws SQLException {
        runner.update(statement.getConnection(), deleteSQL, viewId);
        return true;
    }

    /**
     * {@inheritDoc}
     * @param statement
     */
    @Override
    public Collection<MaterialView> listAll(Statement statement) throws SQLException {
        return runner.query(statement.getConnection(), selectSQL, new MaterialViewResultSetListHandler());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MaterialView get(Statement statement, String viewId) throws SQLException {
        String sql = selectSQL.concat(" where viewName=?");
        return runner.query(statement.getConnection(), sql, new MaterialViewResultSetHandler(), new Object[]{viewId});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<MaterialView> getMaterialViewListByFactTable(Statement statement, String factTable) throws SQLException {
        String sql = selectSQL.concat(" where factTable=?");
        return runner.query(statement.getConnection(), sql, new MaterialViewResultSetListHandler(), new Object[]{factTable});
    }

    /**
     * {@inheritDoc}
     * @param statement
     */
    @Override
    public boolean showMaterialViews(Statement statement) throws SQLException {
        String sql = "select viewName from " + TABLE_NAME;
        return statement.execute(sql);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean showCreateMaterialView(Statement statement, String viewName) throws SQLException {
        String sql = "select createSQL from " + TABLE_NAME + " where viewName='" + viewName + "'";
        return statement.execute(sql);
    }

    /**
     * 空ResultSetHandler
     */
    public class MaterialViewResultSetEmptyHandler implements ResultSetHandler {
        @Override
        public Object handle(ResultSet rs) throws SQLException {
            return null;
        }
    }

    /**
     * 物化视图ResultSetListHandler
     */
    public class MaterialViewResultSetListHandler implements ResultSetHandler<Collection<MaterialView>> {
        @Override
        public Collection<MaterialView> handle(ResultSet rs) throws SQLException {
            Collection<MaterialView> results = new ArrayList<MaterialView>();
            while (rs.next()) {

                MaterialView mv = new MaterialView();
                String viewName = rs.getString("viewName");
                String buildStr = rs.getString("build");
                String refreshStr = rs.getString("refresh");
                boolean rewrite = rs.getBoolean("rewrite");
                String sqlStr = rs.getString("sqlStr");
                String factTable = rs.getString("factTable");
                byte[] sqlDataStruct = rs.getBytes("sqlDataStruct");
                boolean store = rs.getBoolean("store");
                String createSQL = rs.getString("createSQL");


                mv.setViewName(viewName);
                mv.setBuild(MaterialView.Build.valueOf(buildStr));
                mv.setRefresh(MaterialView.Refresh.valueOf(refreshStr));
                mv.setRewrite(rewrite);
                mv.setSqlStr(sqlStr);
                mv.setFactTable(factTable);
                SqlExpression sqlExpression = SerializeHelper.deserialize(sqlDataStruct);
                mv.setSqlDataStruct(sqlExpression);
                mv.setStore(store);
                mv.setCreateSQL(createSQL);

                results.add(mv);
            }
            return results;
        }
    }

    /**
     * 物化视图ResultSetHandler
     */
    public class MaterialViewResultSetHandler implements ResultSetHandler<MaterialView> {

        @Override
        public MaterialView handle(ResultSet rs) throws SQLException {
            MaterialView mv = null;
            while (rs.next()) {
                mv = new MaterialView();
                String viewName = rs.getString("viewName");
                String buildStr = rs.getString("build");
                String refreshStr = rs.getString("refresh");
                boolean rewrite = rs.getBoolean("rewrite");
                String sqlStr = rs.getString("sqlStr");
                String factTable = rs.getString("factTable");
                byte[] sqlDataStruct = rs.getBytes("sqlDataStruct");
                boolean store = rs.getBoolean("store");
                String createSQL = rs.getString("createSQL");

                mv.setViewName(viewName);
                mv.setBuild(MaterialView.Build.valueOf(buildStr));
                mv.setRefresh(MaterialView.Refresh.valueOf(refreshStr));
                mv.setRewrite(rewrite);
                mv.setSqlStr(sqlStr);
                mv.setFactTable(factTable);
                SqlExpression sqlExpression = SerializeHelper.deserialize(sqlDataStruct);
                mv.setSqlDataStruct(sqlExpression);
                mv.setStore(store);
                mv.setCreateSQL(createSQL);
            }
            return mv;
        }
    }
}