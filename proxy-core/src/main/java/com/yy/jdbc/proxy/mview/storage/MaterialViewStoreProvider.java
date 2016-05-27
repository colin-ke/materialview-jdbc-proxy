package com.yy.jdbc.proxy.mview.storage;

import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.mview.common.storage.IMaterialViewStoreProvider;
import com.yy.jdbc.proxy.mview.common.storage.IMaterialViewStoreRepository;

import java.sql.SQLException;
import java.sql.Statement;

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
    public boolean build(Statement statement, MaterialView view)
            throws SQLException {
        return repository.build(statement, view);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean refresh(Statement statement, MaterialView view)
            throws SQLException {
        return repository.refresh(statement, view);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean drop(Statement statement, MaterialView view)
            throws SQLException {
        return repository.drop(statement, view);
    }
}