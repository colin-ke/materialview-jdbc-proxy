package com.yy.jdbc.proxy.driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yy.jdbc.proxy.mview.MaterialView;
import com.yy.jdbc.proxy.mview.MaterialViewManager;
import com.yy.jdbc.proxy.sql.parser.SQLParser;

public class ProxyMaterialView {
	protected static Logger logger = LoggerFactory.getLogger(ProxyMaterialView.class);
	private static Map<Connection,Boolean> conRewriteMap=new ConcurrentHashMap<>();
	// 简单判断
	public static enum ViewOpt {
		DROP("drop materialized view "), CREATE("create materialized view "), REFRESH(
				"refresh materialized view ");
		private final String optPrefix;
		
		ViewOpt(String optPrefix) {
			this.optPrefix = optPrefix;
		}

		public boolean isMatchViewOpt(String sql) {
			return !StringUtils.isEmpty(sql)
					&& sql.toLowerCase().trim().replaceAll("\\s+", " ")
							.startsWith(this.optPrefix);
		}
		public String getOptPrefix(){
			return this.optPrefix;
		}

		public static ViewOpt getViewOpt(String sql) {
			if (ViewOpt.DROP.isMatchViewOpt(sql))
				return ViewOpt.DROP;
			else if (ViewOpt.CREATE.isMatchViewOpt(sql))
				return ViewOpt.CREATE;
			else if (ViewOpt.REFRESH.isMatchViewOpt(sql))
				return ViewOpt.REFRESH;
			else
				return null;
		}
	}

	public static boolean proxyExecute(final Statement statement,
			String sql, boolean isSupportMaterialView) throws SQLException {
		if (isSupportMaterialView) {
			switch (ViewOpt.getViewOpt(sql)) {
			case REFRESH:
				throw new SQLException("数据库不支持刷新物化视图命令!");
			case CREATE:
			case DROP:
			default:
				return statement.execute(sql);
			}
		} else {
			if(autoRewrite(sql, statement.getConnection())){
				return true;
			}
			MaterialView model = null;
			ViewOpt viewSqlType=ViewOpt.getViewOpt(sql);
			if(viewSqlType!=null){
				try{
					model =SQLParser.genMaterialView(sql);
				}catch(Exception e){
					e.printStackTrace();
					return statement.execute(sql);
				}
			}else{
				return 	statement.execute(sql);
			}
			switch (viewSqlType) {
			case CREATE:
				model.setSqlDataStruct(null);
				return MaterialViewManager.createMaterialView(model, statement.getConnection());
			case DROP:
				return MaterialViewManager.dropMaterialView(model.getViewName(), statement.getConnection());
			case REFRESH:
				return MaterialViewManager.refreshMaterialView(model.getViewName(), statement.getConnection());
			default:
				throw new SQLException("不可能发生...");
			}
		}
	}

	private static Collection<MaterialView> listMaterialViews(
			final Statement statement, String factTableName)
			throws SQLException {
		MaterialView model = new MaterialView();
		model.setFactTable(factTableName);
		return MaterialViewManager.getMaterialViewListByFactTable(model.getFactTable(),statement.getConnection());
	}

	/**
	 * 第一步:解析sql,判断查询sql是否基于星型模型</br>
	 * 第二步:第一步的前提下,判断sql是否存在可以查询的物化视图(rewrite为true) 是,则做match,否,则直接返回sql</br>
	 * 第三步:第二步能match到最优的物化视图,则通过此物化视图的数据结构和查询sql的数据结构, 重写sql </br>
	 * 
	 * @param sql
	 * @param statement
	 * @param isSupportMaterialView
	 * @return
	 * @throws SQLException
	 */
	public static ResultSet proxyExecute(String sql,
			Statement statement, boolean isSupportMaterialView)
			throws SQLException {
		if(StringUtils.isEmpty(sql)) throw new SQLException("sql不能为空");
		if(formatSql(sql).equals("show mviews")){
			return MaterialViewManager.showMaterialViews(statement.getConnection());
		}
		
		if(formatSql(sql).startsWith("show create mview")){
			String viewName=formatSql(sql).replace("show create mview ","");
			return MaterialViewManager.showCreateMaterialView(statement.getConnection(),viewName);
		}
		
		if(!sql.trim().toLowerCase().startsWith("select "))return statement.executeQuery(sql);
		if (isSupportMaterialView) {
			return statement.executeQuery(sql);
		} else {
			Boolean isRewrite=conRewriteMap.get(statement.getConnection());
			if(isRewrite==null||!isRewrite){
				return statement.executeQuery(sql);
			}
			
			MaterialView model=null;
			try{
				model =SQLParser.genMaterialView(sql);
			}catch(Exception e){
				e.printStackTrace();
				return statement.executeQuery(sql);
			}
			Collection<MaterialView> materViews = ProxyMaterialView
					.listMaterialViews(statement, model.getFactTable());
			if (CollectionUtils.isEmpty(materViews)) {
				return statement.executeQuery(sql);
			} else {
				Iterator<MaterialView> iter=materViews.iterator();
				while(iter.hasNext()){
					MaterialView view=iter.next();
					MaterialView viewModel =SQLParser.genMaterialView(view.getCreateSQL());
					sql=model.getSqlDataStruct().tryRewrite(viewModel.getSqlDataStruct(), view.getViewName()).toSql();
					if(!model.getSqlDataStruct().toSql().equals(sql)){
						logger.info("rewrite sql to:"+sql);
						break;
					}
				}
				return statement.executeQuery(sql);
			}

		}
	}
	
	private static String formatSql(String sql){
		return sql.toLowerCase().replaceAll("\\s+|;", " ").trim();
	}

	private static boolean autoRewrite(String sql,Connection con){
		if(formatSql(sql).equals("set auto_rewrite=on")){
			conRewriteMap.put(con,true);
			logger.info(sql);
			return true;
		}else if(formatSql(sql).equals("set auto_rewrite=off")){
			conRewriteMap.put(con,false);
			logger.info(sql);
			return true;
		}
		return false;
	}
}
