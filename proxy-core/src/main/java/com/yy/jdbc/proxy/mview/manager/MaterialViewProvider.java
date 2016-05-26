package com.yy.jdbc.proxy.mview.manager;

import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.mview.common.manager.IMaterialViewProvider;
import com.yy.jdbc.proxy.mview.common.manager.IMaterialViewRepository;
import com.yy.jdbc.proxy.mview.common.manager.MaterialViewAlreadyExistException;
import com.yy.jdbc.proxy.mview.common.manager.MaterialViewNotExistException;
import com.yy.jdbc.proxy.mview.common.storage.IMaterialViewStoreProvider;
import com.yy.jdbc.proxy.mview.storage.MaterialViewStoreProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

/**
 * 物化视图服务
 *
 * @author ZhangXueJun
 */
public class MaterialViewProvider implements IMaterialViewProvider {

    private static final Logger logger = LoggerFactory.getLogger(
            MaterialViewProvider.class);

    private IMaterialViewRepository repository = new MaterialViewRepository();
    private IMaterialViewStoreProvider storeProvider = new MaterialViewStoreProvider();

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isExists(Connection conn, String viewName)
            throws SQLException {
        boolean result = repository.isExists(conn, viewName);
        if (result) {
            SQLException exception = new MaterialViewAlreadyExistException(viewName);
            logger.error(exception.getMessage());
            throw exception;
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean create(Connection conn, MaterialView view) throws SQLException {
        if (!isExists(conn, view.getViewName())) {
            if (view.getBuild() == MaterialView.Build.immediate && !view.isStore()) {
                storeProvider.build(conn, view);
                view.setStore(true);
            }
            return repository.create(conn, view);
        }
        return false;
    }

    //    @Override
    public boolean update(Connection conn, MaterialView view) throws SQLException {
        return ((MaterialViewRepository) repository).update(conn, view);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete(Connection conn, String viewName) throws SQLException {
        MaterialView mv = get(conn, viewName);
        if (mv != null) {
            storeProvider.drop(conn, mv);
        }
        return repository.delete(conn, viewName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<MaterialView> listAll(Connection conn) throws SQLException {
        return repository.listAll(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MaterialView get(Connection conn, String viewName) throws SQLException {
        MaterialView view = repository.get(conn, viewName);
        if (view == null) {
            SQLException exception = new MaterialViewNotExistException(viewName);
            logger.info(exception.getMessage());
            throw exception;
        }
        return refreshIfNecessary(conn, view);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<MaterialView> getMaterialViewListByFactTable(
            Connection conn, String factTable) throws SQLException {
        Collection<MaterialView> views = repository.
                getMaterialViewListByFactTable(conn, factTable);
        for (MaterialView view : views) {
            if (!view.isStore()) {
                refreshIfNecessary(conn, view);
            }
        }
        return views;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean refresh(Connection conn, String viewName) throws SQLException {
        MaterialView view = get(conn, viewName);
        return storeProvider.refresh(conn, view);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet showMaterialViews(Connection conn) throws SQLException {
        return repository.showMaterialViews(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet showCreateMaterialView(Connection conn, String viewName) throws SQLException {
        return repository.showCreateMaterialView(conn, viewName);
    }

    /**
     * 如果有必要才刷新
     *
     * @param conn
     * @param view
     * @return
     */
    private MaterialView refreshIfNecessary(Connection conn, MaterialView view)
            throws SQLException {
        if (view.getBuild() == MaterialView.Build.deferred && !view.isStore()) {
            storeProvider.build(conn, view);
            view.setStore(true);
            update(conn, view);
        }
        return view;
    }
}