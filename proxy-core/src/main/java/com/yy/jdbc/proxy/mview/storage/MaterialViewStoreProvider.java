package com.yy.jdbc.proxy.mview.storage;

import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.mview.common.storage.IMaterialViewStoreProvider;
import com.yy.jdbc.proxy.mview.common.storage.IMaterialViewStoreRepository;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 物化视图数据存储服务
 *
 * @author ZhangXueJun
 */
public class MaterialViewStoreProvider implements IMaterialViewStoreProvider {

    private IMaterialViewStoreRepository repository = new MaterialViewStoreRepository();

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean build(Connection conn, MaterialView view)
            throws SQLException {
        return repository.build(conn, view);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean refresh(Connection conn, MaterialView view)
            throws SQLException {
        return repository.refresh(conn, view);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean drop(Connection conn, MaterialView view)
            throws SQLException {
        return repository.drop(conn, view);
    }
}