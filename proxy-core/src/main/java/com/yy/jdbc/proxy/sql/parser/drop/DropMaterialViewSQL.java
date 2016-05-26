package com.yy.jdbc.proxy.sql.parser.drop;

import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.stmt.TUnknownSqlStatement;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yy.jdbc.proxy.driver.ProxyMaterialView.ViewOpt;
import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.sql.parser.SQL;
import com.yy.jdbc.proxy.sql.parser.select.SelectSQL;

public class DropMaterialViewSQL implements SQL {
	public static Log selectSqlLog=LogFactory.getLog(SelectSQL.class);
	private TUnknownSqlStatement dropMaterialViewStmt;

	@Override
	public SQLType getSQLType() {
		return SQLType.DROP_MATERIAL_VIEW;
	}

	public TUnknownSqlStatement getSelectStmt() {
		return dropMaterialViewStmt;
	}

	/**
	 * 当前gsqlparser版本没有直接解析出tablename,当成unknow sql特殊处理
	 * */
	@Override
	public Object compile() throws SQLException{
		String tableName=dropMaterialViewStmt.toString().toLowerCase().trim().replaceAll("\\s+", " ").replace(";","").replace(ViewOpt.DROP.getOptPrefix(), "").trim();
		MaterialView dropView=new MaterialView();
		dropView.setViewName(tableName);
		return dropView;
	}

	@Override
	public TCustomSqlStatement getSQLStmt() {
		return this.dropMaterialViewStmt;
	}

	@Override
	public void setSQLStmt(TCustomSqlStatement sqlStatement) {
		this.dropMaterialViewStmt=(TUnknownSqlStatement)sqlStatement;
	}
}
