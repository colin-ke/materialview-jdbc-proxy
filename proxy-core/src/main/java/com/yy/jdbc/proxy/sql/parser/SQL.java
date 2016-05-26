package com.yy.jdbc.proxy.sql.parser;

import gudusoft.gsqlparser.TCustomSqlStatement;

import java.sql.SQLException;

public interface SQL {
	public static enum SQLType{SELECT,INSERT,DROP,DROP_MATERIAL_VIEW,REPLACE,TRUNCATE,UPDATE,DELETE,ALTER,CREATE,CREATE_MATERIAL_VIEW}
	public Object compile() throws SQLException;
	public SQLType getSQLType();
	public TCustomSqlStatement getSQLStmt();
	public void setSQLStmt(TCustomSqlStatement stmt);
	
}
