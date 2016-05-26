package com.yy.jdbc.proxy.driver;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mysql.jdbc.ConnectionPropertiesTransform;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.Util;


public class ProxyDriver implements Driver {
	private static Log LOG = LogFactory.getLog(ProxyDriver.class);
	private Driver driver;
	private ProxyConnection connection;
	private boolean isSupportMaterialView;
	static {
		try {
			DriverManager.registerDriver(new ProxyDriver());
		} catch (SQLException e) {
			LOG.error("Driver register failed!",e);
		}
	}

	private void loadDriverIfNotExist(String url) throws SQLException{
		if(driver==null){
			this.driver=ProxyJDBCDriverFactory.buildDriver(url);
			this.isSupportMaterialView=ProxyJDBCDriverFactory.isSupportMaterialView(url);
		}
	}
	
	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		loadDriverIfNotExist(url);
		String[] urlParts=url.split(":");
		urlParts[1]=urlParts[1].toLowerCase().split("_")[1];
		url=StringUtils.join(urlParts,":");
		this.connection=new ProxyConnection(this.driver.connect(url, info),isSupportMaterialView);
		return this.connection;
	}

	
	
	@Override
	public boolean acceptsURL(String url) throws SQLException {
		if(StringUtils.isEmpty(url)){
			throw new SQLException("driver link can not be empty.");
		}
		String[] protocolPaths=url.split(":");
		if(protocolPaths[1]!=null){
			String[] dbProtocol=protocolPaths[1].toUpperCase().split("_");
			if(dbProtocol.length!=2||!"PROXY".equals(dbProtocol[0])){
				return false;
			}
			return true;
		}
		return false;
	}
	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
			throws SQLException {
		loadDriverIfNotExist(url);
	    if (info == null) {
	        info = new Properties();
	      }

	      if ((url != null)) {
	        info = parseURL(url, info);
	      }

	      DriverPropertyInfo hostProp = new DriverPropertyInfo("HOST", info.getProperty("HOST"));

	      DriverPropertyInfo portProp = new DriverPropertyInfo("PORT", info.getProperty("PORT"));

	      DriverPropertyInfo dbProp = new DriverPropertyInfo("DBNAME", info.getProperty("DBNAME"));

	      DriverPropertyInfo userProp = new DriverPropertyInfo("user", info.getProperty("user"));

	      DriverPropertyInfo passwordProp = new DriverPropertyInfo("password", info.getProperty("password"));

	      DriverPropertyInfo[] dpi = new DriverPropertyInfo[5];

	      dpi[0] = hostProp;
	      dpi[1] = portProp;
	      dpi[2] = dbProp;
	      dpi[3] = userProp;
	      dpi[4] = passwordProp;

	      return dpi;
	}
	
	public Properties parseURL(String url, Properties defaults) throws SQLException
	  {
	    Properties urlProps = new Properties();

	    if (url == null) {
	      return null;
	    }

	    int beginningOfSlashes = url.indexOf("//");

	    int index = url.indexOf("?");

	    if (index != -1) {
	      String paramString = url.substring(index + 1, url.length());
	      url = url.substring(0, index);

	      StringTokenizer queryParams = new StringTokenizer(paramString, "&");

	      while (queryParams.hasMoreTokens()) {
	        String parameterValuePair = queryParams.nextToken();

	        int indexOfEquals = StrUtil.indexOfIgnoreCase(0, parameterValuePair, "=");

	        String parameter = null;
	        String value = null;

	        if (indexOfEquals != -1) {
	          parameter = parameterValuePair.substring(0, indexOfEquals);

	          if (indexOfEquals + 1 < parameterValuePair.length()) {
	            value = parameterValuePair.substring(indexOfEquals + 1);
	          }
	        }

	        if ((value != null) && (value.length() > 0) && (parameter != null) && (parameter.length() > 0)) {
	          try
	          {
	            urlProps.put(parameter, URLDecoder.decode(value, "UTF-8"));
	          }
	          catch (UnsupportedEncodingException badEncoding)
	          {
	            urlProps.put(parameter, URLDecoder.decode(value));
	          }
	          catch (NoSuchMethodError nsme) {
	            urlProps.put(parameter, URLDecoder.decode(value));
	          }
	        }
	      }
	    }

	    url = url.substring(beginningOfSlashes + 2);

	    String hostStuff = null;

	    int slashIndex = StrUtil.indexOfIgnoreCaseRespectMarker(0, url, "/", "\"'", "\"'", true);

	    if (slashIndex != -1) {
	      hostStuff = url.substring(0, slashIndex);

	      if (slashIndex + 1 < url.length())
	        urlProps.put("DBNAME", url.substring(slashIndex + 1, url.length()));
	    }
	    else
	    {
	      hostStuff = url;
	    }

	    int numHosts = 0;

	    if ((hostStuff != null) && (hostStuff.trim().length() > 0)) {
	      List<String> hosts = StrUtil.split(hostStuff, ",", "\"'", "\"'", false);

	      for (String hostAndPort : hosts) {
	        ++numHosts;

	        String[] hostPortPair = parseHostPortPair(hostAndPort);

	        if ((hostPortPair[0] != null) && (hostPortPair[0].trim().length() > 0))
	          urlProps.setProperty("HOST." + numHosts, hostPortPair[0]);
	        else {
	          urlProps.setProperty("HOST." + numHosts, "localhost");
	        }

	        if (hostPortPair[1] != null)
	          urlProps.setProperty("PORT." + numHosts, hostPortPair[1]);
	        else
	          urlProps.setProperty("PORT." + numHosts, "3306");
	      }
	    }
	    else {
	      numHosts = 1;
	      urlProps.setProperty("HOST.1", "localhost");
	      urlProps.setProperty("PORT.1", "3306");
	    }

	    urlProps.setProperty("NUM_HOSTS", String.valueOf(numHosts));
	    urlProps.setProperty("HOST", urlProps.getProperty("HOST.1"));
	    urlProps.setProperty("PORT", urlProps.getProperty("PORT.1"));

	    String propertiesTransformClassName = urlProps.getProperty("propertiesTransform");

	    if (propertiesTransformClassName != null) {
	      try {
	        ConnectionPropertiesTransform propTransformer = (ConnectionPropertiesTransform)Class.forName(propertiesTransformClassName).newInstance();

	        urlProps = propTransformer.transformProperties(urlProps);
	      } catch (InstantiationException e) {
	        throw SQLError.createSQLException("Unable to create properties transform instance '" + propertiesTransformClassName + "' due to underlying exception: " + e.toString(), "01S00", null);
	      }
	      catch (IllegalAccessException e)
	      {
	        throw SQLError.createSQLException("Unable to create properties transform instance '" + propertiesTransformClassName + "' due to underlying exception: " + e.toString(), "01S00", null);
	      }
	      catch (ClassNotFoundException e)
	      {
	        throw SQLError.createSQLException("Unable to create properties transform instance '" + propertiesTransformClassName + "' due to underlying exception: " + e.toString(), "01S00", null);
	      }

	    }

	    if ((Util.isColdFusion()) && (urlProps.getProperty("autoConfigureForColdFusion", "true").equalsIgnoreCase("true")))
	    {
	      String configs = urlProps.getProperty("useConfigs");

	      StringBuffer newConfigs = new StringBuffer();

	      if (configs != null) {
	        newConfigs.append(configs);
	        newConfigs.append(",");
	      }

	      newConfigs.append("coldFusion");

	      urlProps.setProperty("useConfigs", newConfigs.toString());
	    }

	    String configNames = null;

	    if (defaults != null) {
	      configNames = defaults.getProperty("useConfigs");
	    }

	    if (configNames == null) {
	      configNames = urlProps.getProperty("useConfigs");
	    }

	    if (configNames != null) {
	      List splitNames = StrUtil.split(configNames, ",", true);

	      Properties configProps = new Properties();

	      Iterator namesIter = splitNames.iterator();

	      while (namesIter.hasNext()) {
	        String configName = (String)namesIter.next();
	        try
	        {
	          InputStream configAsStream = super.getClass().getResourceAsStream("configs/" + configName + ".properties");

	          if (configAsStream == null) {
	            throw SQLError.createSQLException("Can't find configuration template named '" + configName + "'", "01S00", null);
	          }

	          configProps.load(configAsStream);
	        } catch (IOException ioEx) {
	          SQLException sqlEx = SQLError.createSQLException("Unable to load configuration template '" + configName + "' due to underlying IOException: " + ioEx, "01S00", null);

	          sqlEx.initCause(ioEx);

	          throw sqlEx;
	        }
	      }

	      Iterator propsIter = urlProps.keySet().iterator();

	      while (propsIter.hasNext()) {
	        String key = propsIter.next().toString();
	        String property = urlProps.getProperty(key);
	        configProps.setProperty(key, property);
	      }

	      urlProps = configProps;
	    }

	    if (defaults != null) {
	      Iterator propsIter = defaults.keySet().iterator();

	      while (propsIter.hasNext()) {
	        String key = propsIter.next().toString();
	        if (!(key.equals("NUM_HOSTS"))) {
	          String property = defaults.getProperty(key);
	          urlProps.setProperty(key, property);
	        }
	      }
	    }

	    return urlProps;
	  }
	protected static String[] parseHostPortPair(String hostPortPair)
		    throws SQLException
		  {
		    String[] splitValues = new String[2];

		    if (StrUtil.startsWithIgnoreCaseAndWs(hostPortPair, "address")) {
		      splitValues[0] = hostPortPair.trim();
		      splitValues[1] = null;

		      return splitValues;
		    }

		    int portIndex = hostPortPair.indexOf(":");

		    String hostname = null;

		    if (portIndex != -1) {
		      if (portIndex + 1 < hostPortPair.length()) {
		        String portAsString = hostPortPair.substring(portIndex + 1);
		        hostname = hostPortPair.substring(0, portIndex);

		        splitValues[0] = hostname;

		        splitValues[1] = portAsString;
		         }
		     }

		    splitValues[0] = hostPortPair;
		    splitValues[1] = null;

		    return splitValues;
		  }
	@Override
	public int getMajorVersion() {
		if(driver!=null) 
			return driver.getMajorVersion();
		else
			return 0;
	}
	@Override
	public int getMinorVersion() {
		if(driver!=null) 
			return driver.getMinorVersion();
		else
			return 0;
	}
	@Override
	public boolean jdbcCompliant() {
		if(driver!=null) 
			return driver.jdbcCompliant();
		else
			return false;
	}
	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		if(driver!=null) 
			return driver.getParentLogger();
		else
			return null;
	}

	public boolean isSupportMaterializedView() {
		return isSupportMaterialView;
	}	
}
