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

import java.sql.SQLException;
import java.sql.Statement;
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
    public boolean isExists(Statement statement, String viewName)
            throws SQLException {
        boolean result = repository.isExists(statement, viewName);
        if (result) {
            SQLException exception = new MaterialViewAlreadyExistException(viewName);
            logger.error(exception.getMessage());
            throw exception;
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * @param statement
     * @param view
     */
    @Override
    public boolean create(Statement statement, MaterialView view) throws SQLException {
        if (!isExists(statement, view.getViewName())) {
            if (view.getBuild() == MaterialView.Build.immediate && !view.isStore()) {
                storeProvider.build(statement, view);
                view.setStore(true);
            }
            return repository.create(statement, view);
        }
        return false;
    }

    //    @Override
    public boolean update(Statement statement, MaterialView view) throws SQLException {
        return ((MaterialViewRepository) repository).update(statement, view);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean delete(Statement statement, String viewName) throws SQLException {
        MaterialView mv = get(statement, viewName);
        if (mv != null) {
            storeProvider.drop(statement, mv);
        }
        return repository.delete(statement, viewName);
    }

    /**
     * {@inheritDoc}
     * @param statement
     */
    @Override
    public Collection<MaterialView> listAll(Statement statement) throws SQLException {
        return repository.listAll(statement);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MaterialView get(Statement statement, String viewName) throws SQLException {
        MaterialView view = repository.get(statement, viewName);
        if (view == null) {
            SQLException exception = new MaterialViewNotExistException(viewName);
            logger.info(exception.getMessage());
            throw exception;
        }
        return refreshIfNecessary(statement, view);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<MaterialView> getMaterialViewListByFactTable(
            Statement statement, String factTable) throws SQLException {
        Collection<MaterialView> views = repository.
                getMaterialViewListByFactTable(statement, factTable);
        for (MaterialView view : views) {
            if (!view.isStore()) {
                refreshIfNecessary(statement, view);
            }
        }
        return views;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean refresh(Statement statement, String viewName) throws SQLException {
        MaterialView view = get(statement, viewName);
        return storeProvider.refresh(statement, view);
    }

    /**
     * {@inheritDoc}
     * @param statement
     */
    @Override
    public boolean showMaterialViews(Statement statement) throws SQLException {
        return repository.showMaterialViews(statement);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean showCreateMaterialView(Statement statement, String viewName) throws SQLException {
        return repository.showCreateMaterialView(statement, viewName);
    }

    /**
     * 如果有必要才刷新
     *
     * @param statement
     * @param view
     * @return
     */
    private MaterialView refreshIfNecessary(Statement statement, MaterialView view)
            throws SQLException {
        if (view.getBuild() == MaterialView.Build.deferred && !view.isStore()) {
            storeProvider.build(statement, view);
            view.setStore(true);
            update(statement, view);
        }
        return view;
    }
}