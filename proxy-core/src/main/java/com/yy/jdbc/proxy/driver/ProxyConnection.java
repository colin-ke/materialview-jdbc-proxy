package com.yy.jdbc.proxy.driver;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyConnection implements Connection {
	private Connection con;
	private boolean isSupportMaterialView;
	public ProxyConnection(Connection con, boolean isSupportMaterialView) {
		this.con=con;
		this.isSupportMaterialView=isSupportMaterialView;
	}
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return con.unwrap(iface);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return this.con.isWrapperFor(iface);
	}

	@Override
	public Statement createStatement() throws SQLException {
		return new ProxyStatement(this.con.createStatement(),this.isSupportMaterialView);
	}

	protected static Logger logger = LoggerFactory.getLogger(ProxyConnection.class);
	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		logger.info("prepareStatement:"+sql);
		return this.con.prepareStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		logger.info("prepareCall:"+sql);
		return this.con.prepareCall(sql);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return this.con.nativeSQL(sql);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		this.con.setAutoCommit(autoCommit);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		return this.con.getAutoCommit();
	}

	@Override
	public void commit() throws SQLException {
		this.con.commit();
	}

	@Override
	public void rollback() throws SQLException {
		this.con.rollback();
	}

	@Override
	public void close() throws SQLException {
		this.con.close();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return this.con.isClosed();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		return this.con.getMetaData();
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		this.con.setReadOnly(readOnly);
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return this.con.isReadOnly();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		this.con.setCatalog(catalog);
	}

	@Override
	public String getCatalog() throws SQLException {
		return con.getCatalog();
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		this.con.setTransactionIsolation(level);
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		return this.con.getTransactionIsolation();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return this.con.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		this.con.clearWarnings();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		return  new ProxyStatement(this.con.createStatement(resultSetType, resultSetConcurrency),this.isSupportMaterialView);
		
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {

		logger.info("prepareStatement(String sql, int resultSetType,int resultSetConcurrency):"+sql);
		return this.con.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {

		logger.info("prepareCall(String sql, int resultSetType,int resultSetConcurrency):"+sql);
		return this.con.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return this.con.getTypeMap();
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		this.con.setTypeMap(map);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		this.con.setHoldability(holdability);
	}

	@Override
	public int getHoldability() throws SQLException {
		return this.con.getHoldability();
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		return this.con.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		return this.con.setSavepoint(name);
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		this.con.rollback(savepoint);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		this.con.releaseSavepoint(savepoint);
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return  new ProxyStatement(this.con.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability),this.isSupportMaterialView);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		logger.info("prepareStatement(String sql, int resultSetType,int resultSetConcurrency, int resultSetHoldability)"+sql);
		
		return this.con.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return this.con.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		return this.con.prepareStatement(sql, autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		return this.con.prepareStatement(sql, columnIndexes);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		return this.con.prepareStatement(sql, columnNames);
	}

	@Override
	public Clob createClob() throws SQLException {
		return this.con.createClob();
	}

	@Override
	public Blob createBlob() throws SQLException {
		return this.con.createBlob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		return this.con.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		return this.con.createSQLXML();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		return this.con.isValid(timeout);
	}

	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		this.con.setClientInfo(name, value);
	}

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		this.con.setClientInfo(properties);
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		return this.con.getClientInfo(name);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return this.con.getClientInfo();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		return this.con.createArrayOf(typeName, elements);
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		return this.con.createStruct(typeName, attributes);
	}

	@Override
	public void setSchema(String schema) throws SQLException {
		this.con.setSchema(schema);
	}

	@Override
	public String getSchema() throws SQLException {
		return this.con.getSchema();
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		this.con.abort(executor);
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		this.con.setNetworkTimeout(executor, milliseconds);
	}

	@Override
	public int getNetworkTimeout() throws SQLException {
		return this.con.getNetworkTimeout();
	}

}
