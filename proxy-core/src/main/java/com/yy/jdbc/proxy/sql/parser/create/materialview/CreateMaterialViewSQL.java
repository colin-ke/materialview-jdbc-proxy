package com.yy.jdbc.proxy.sql.parser.create.materialview;

import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TSourceToken;
import gudusoft.gsqlparser.TSourceTokenList;
import gudusoft.gsqlparser.pp.para.GFmtOptFactory;
import gudusoft.gsqlparser.pp.stmtformatter.SqlFormatter;
import gudusoft.gsqlparser.stmt.TCreateMaterializedSqlStatement;

import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.sql.parser.SQL;
import com.yy.jdbc.proxy.sql.parser.SQLParser;
import com.yy.jdbc.proxy.sql.parser.select.SelectSQL;

public class CreateMaterialViewSQL implements SQL {
	public static Log selectSqlLog=LogFactory.getLog(SelectSQL.class);
	private TCreateMaterializedSqlStatement createMaterialViewStmt;

	@Override
	public SQLType getSQLType() {
		return SQLType.DROP_MATERIAL_VIEW;
	}

	public TCreateMaterializedSqlStatement getSelectStmt() {
		return createMaterialViewStmt;
	}

	/**
	 * 当前gsqlparser版本没有直接解析出create materialized view 的每个参数部分，
	 * 所以直接使用它的分词之后的列表直接特殊取值
	 * */
	@Override
	public Object compile() throws SQLException{
		MaterialView materialView=new MaterialView();
		materialView.setViewName(createMaterialViewStmt.getViewName().toString());
		TSourceTokenList list=createMaterialViewStmt.sourcetokenlist;
		loop:while(list.hasNext()){
			TSourceToken token=list.next();
			switch (token.toString().toLowerCase()) {
			case "build":
				list.hasNext();
				list.next();
				list.hasNext();
				materialView.setBuild(MaterialView.Build.valueOf(list.next().toString().toLowerCase()));
				break;
			case "refresh":
				list.hasNext();
				list.next();
				list.hasNext();
				materialView.setRefresh(MaterialView.Refresh.valueOf(list.next().toString().toLowerCase()));
				break;
			case "enable":
				list.hasNext();
				list.next();
				list.hasNext();
				if(list.next().toString().toLowerCase().equals("query")){
					materialView.setRewrite(true);
				}
				break;
			case "select":
				break loop;
			default:
			}
		}
		SqlFormatter sqlFormatter=new SqlFormatter();
		String formatSql=sqlFormatter.format(createMaterialViewStmt.getGsqlparser(),new GFmtOptFactory().newInstance());
		TCustomSqlStatement sqlStmt=createMaterialViewStmt.getSubquery();
		MaterialView selectView=null;
		try{
			SelectSQL selectSQL=(SelectSQL)SQLParser.getAbstractSQL(sqlStmt.sqlstatementtype,sqlStmt);
			if(selectSQL.hasHaving()){
				throw new SQLException("创建视图语句不能有having条件！");
			}
			selectView=(MaterialView)selectSQL.compile();
		}catch(Exception e){
			e.printStackTrace();
			throw new SQLException(String.format("创建物化视图失败:%s",e.getMessage()));
		}
		materialView.setSqlStr(selectView.getSqlStr());
		materialView.setCreateSQL(formatSql);
		materialView.setSqlDataStruct(selectView.getSqlDataStruct());
		materialView.setFactTable(selectView.getFactTable());
		return materialView;
	}

	@Override
	public TCustomSqlStatement getSQLStmt() {
		return this.createMaterialViewStmt;
	}

	@Override
	public void setSQLStmt(TCustomSqlStatement sqlStatement) {
		this.createMaterialViewStmt=(TCreateMaterializedSqlStatement)sqlStatement;
	}
}
