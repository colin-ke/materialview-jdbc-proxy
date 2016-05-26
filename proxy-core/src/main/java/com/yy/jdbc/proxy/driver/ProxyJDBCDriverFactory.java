package com.yy.jdbc.proxy.driver;

import java.lang.reflect.Constructor;
import java.sql.Driver;
import java.sql.SQLException;

import org.apache.commons.lang3.StringUtils;

public class ProxyJDBCDriverFactory {
	public static Driver buildDriver(String driverLink) throws SQLException{
		if(StringUtils.isEmpty(driverLink)){
			throw new SQLException("driver link can not be empty.");
		}
		String[] protocolPaths=driverLink.split(":");
		if(protocolPaths[1]!=null){
			DB db=null;
			try{
				String[] dbProtocol=protocolPaths[1].toUpperCase().split("_");
				if(dbProtocol.length!=2&&!"PROXY".equals(dbProtocol[0])){
					throw new Exception();
				}
				db=DB.valueOf(dbProtocol[1]);
				@SuppressWarnings("unchecked")
				Class<Driver> driverClass = (Class<Driver>) Class
						.forName(db.getDriver());
				Constructor<Driver> driverConstructer=driverClass.getConstructor();
				return driverConstructer.newInstance();
			}catch(IllegalArgumentException e){
				throw new SQLException("driver ["+protocolPaths[1]+"] no support now!");
			}catch (Exception ex){
				throw new SQLException("driver["+db.getDriver()+"]not found in classpath!");
			}
		}else{
			throw new SQLException("driver can not be found.");
		}
	}
	
	public static boolean isSupportMaterialView(String url) throws SQLException{
		if(StringUtils.isEmpty(url)){
			throw new SQLException("driver link can not be empty.");
		}
		String[] protocolPaths=url.split(":");
		if(protocolPaths[1]!=null){
			DB db=null;
			try{
				String[] dbProtocol=protocolPaths[1].toUpperCase().split("_");
				if(dbProtocol.length!=2&&!"PROXY".equals(dbProtocol[0])){
					throw new IllegalArgumentException();
				}
				db=DB.valueOf(dbProtocol[1]);
			}catch(IllegalArgumentException e){
				throw new SQLException("driver ["+protocolPaths[1]+"] no support now!");
			}
			return db.isSupportMaterialView();
		}else{
			throw new SQLException("driver can not be found.");
		}
	}
	
	private static enum DB {
		ORACLE("oracle.jdbc.driver.OracleDriver",true), 
		MYSQL("com.mysql.jdbc.Driver",false);
		private final String driver;
		private final boolean isSupportMaterialView;
		DB(String driver,boolean isSupportMaterialView) {
			this.driver = driver;
			this.isSupportMaterialView=isSupportMaterialView;
		}

		public String getDriver() {
			return driver;
		}

		public boolean isSupportMaterialView() {
			return isSupportMaterialView;
		}
		

	}
}
