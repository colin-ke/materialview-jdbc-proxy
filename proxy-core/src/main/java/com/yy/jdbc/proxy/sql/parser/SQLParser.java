package com.yy.jdbc.proxy.sql.parser;

import com.yy.jdbc.proxy.sql.SqlExpression;
import com.yy.jdbc.proxy.sql.parser.select.SelectSQL;

import gudusoft.gsqlparser.EDbVendor;
import gudusoft.gsqlparser.ESqlStatementType;
import gudusoft.gsqlparser.TCustomSqlStatement;
import gudusoft.gsqlparser.TGSqlParser;
import gudusoft.gsqlparser.stmt.TCreateIndexSqlStatement;
import gudusoft.gsqlparser.stmt.TSelectSqlStatement;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.yy.jdbc.proxy.mview.MaterialView;

public class SQLParser {
	
	public static MaterialView genMaterialView(String sql) throws SQLException{

		//每次必须实例化一个,不然会出错,之前处理过的sql生成的每个token都保存在同一个实例，
		//如果使用了同一个实例，在get到sql每部分信息时，则会出错。
		TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
		sqlparser.setSqltext(sql);
		int resultFlag=sqlparser.parse();
		if(resultFlag==0){
			TCustomSqlStatement customSqlStatement=sqlparser.sqlstatements.get(0);
			try{
				SQL sqlParser=getAbstractSQL(customSqlStatement.sqlstatementtype,customSqlStatement);
				return (MaterialView)sqlParser.compile();
			}catch(Exception e){
				e.printStackTrace();
				throw new SQLException(e.getMessage());
			}
		}else{
			throw new SQLException(sqlparser.getErrormessage());
		}
	}
	
	
	public static SqlExpression genSqlExpression(String sql) throws SQLException {
		if(!sql.toLowerCase().trim().startsWith("select"))
			return null;

		TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvoracle);
		sqlparser.setSqltext(sql);
		int resultFlag=sqlparser.parse();
		if(resultFlag==0){
			TCustomSqlStatement customSqlStatement=sqlparser.sqlstatements.get(0);
			try{
				
				TSelectSqlStatement sqlStatement=(TSelectSqlStatement)customSqlStatement;
				SelectSQL sqlParser= (SelectSQL) getAbstractSQL(sqlStatement.sqlstatementtype,sqlStatement);
				return sqlParser.toSqlExpression();
			}catch(Exception e){
				e.printStackTrace();
				throw new SQLException(e.getMessage());
			}
		}else{
			throw new SQLException(sqlparser.getErrormessage());
		}

	}
	
	
	
	@SuppressWarnings("unchecked")
	public static SQL getAbstractSQL(ESqlStatementType sqlType,
			TCustomSqlStatement sqlStatement) throws SQLException{
		try {
			String clazz=supportSQLType.get(sqlType);
			if(clazz==null){
				throw new SQLException("不支持此类型的sql!");
			}
			Class<SQL> sqlTypeResolverClass = (Class<SQL>) Class
					.forName(clazz);
			Constructor<SQL> sqlTypeResolverConstructor=sqlTypeResolverClass.getConstructor();
			SQL sqlTypeResolver= sqlTypeResolverConstructor.newInstance();
				sqlTypeResolver.setSQLStmt(sqlStatement);
			return sqlTypeResolver;
		} catch (Exception e) {
			e.printStackTrace();
			throw new SQLException(e.getCause());
		}
	}
	
	private static Map<ESqlStatementType,String> supportSQLType =
			new HashMap<ESqlStatementType,String>();
	static {
		supportSQLType.put(ESqlStatementType.sstselect, "com.yy.jdbc.proxy.sql.parser.select.SelectSQL");
		supportSQLType.put(ESqlStatementType.sstoraclecreatematerializedview, "com.yy.jdbc.proxy.sql.parser.create.materialview.CreateMaterialViewSQL");
		supportSQLType.put(ESqlStatementType.sstoracledropmaterializedview, "com.yy.jdbc.proxy.sql.parser.drop.DropMaterialViewSQL");
	}
	
	public static void main(String[] args) throws SQLException {
		TGSqlParser sqlparser = new TGSqlParser(EDbVendor.dbvmysql);
//		String sql="drop materialized view an_user_base_file_no_charge;";
//		String sql="CREATE MATERIALIZED VIEW an_user_base_file_no_charge build  immediate  refresh   complete with primary key ENABLE QUERY REWRITE   AS select distinct user_no from cw_arrearage t where (t.mon = dbms_tianjin.getLastMonth or        t.mon = add_months(dbms_tianjin.getLastMonth, -1))";
//		String sql="select tt1.a,tt2.b,tt3.c,tt4.d from t1 tt1 join t2 tt2 on tt1.a=tt2.a join t3 tt3 on tt3.a=tt1.a join t4 tt4 on tt4.a=tt1.a where (tt1.b=1 and tt2.b=2)or tt3.d=4 group by tt1.c,tt2.c order by tt3.c asc,tt4.c desc";
		String sql="select a from a limit 1,2;";
//		String sql="select aa,bb,cc,sum(kk),count(distinct(ddt)) from tttt where (a=1 and c=2) or c>10 and kk<=100 and mk in (1,2,3) group by mmk order by aaaaaaa asc;";
//		String sql="dbms_mview.refresh(TAB=>'an_user_base_file_no_charge',METHOD=>'FAST',PARALLELISM=>1)";
		sqlparser.setSqltext(sql);
		int resultFlag=sqlparser.parse();
		if(resultFlag==0){
			TSelectSqlStatement sqlStatement=(TSelectSqlStatement)sqlparser.sqlstatements.get(0);
//			SQL sqlParser=getAbstractSQL(sqlStatement.sqlstatementtype,sqlStatement);
			System.out.println(sqlStatement.getLimitClause().getRow_count());
//			MaterialView view=(MaterialView)sqlParser.compile();
//			System.out.println(view);
//			
		}else{
			System.out.println(sqlparser.getErrormessage());
		}
	}
}
