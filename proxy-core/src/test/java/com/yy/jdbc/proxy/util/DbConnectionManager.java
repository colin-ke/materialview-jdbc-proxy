package com.yy.jdbc.proxy.util;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * JDBC连接管理，给予测试
 *
 * @author ZhangXueJun
 */
public class DbConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(DbConnectionManager.class);
    public static ComboPooledDataSource dataSource;

    static {
        InputStream in = ClassLoader.getSystemResourceAsStream("app.properties");
        Properties properties = new Properties();
        try {
            properties.load(in);

            String driver = properties.getProperty("jdbc.className");
            String url = properties.getProperty("jdbc.url");
            String username = properties.getProperty("jdbc.username");
            String password = properties.getProperty("jdbc.password");

            dataSource = init(driver, username, password, url);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static ComboPooledDataSource init(
            String driver,
            String username,
            String password,
            String url)
            throws PropertyVetoException {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setUser(username);
        dataSource.setPassword(password);
        dataSource.setJdbcUrl(url);
        dataSource.setDriverClass(driver);

        dataSource.setInitialPoolSize(10);
        dataSource.setMinPoolSize(5);
        dataSource.setMaxPoolSize(50);
        dataSource.setMaxStatements(100);
        dataSource.setMaxIdleTime(60);
        return dataSource;
    }

    public static void closeConnection(Connection conn) {
        try {
            DbUtils.close(conn);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }

    }

    /**
     * 获取原生数据库连接
     *
     * @return
     */
    public static Connection getConnection() {
//        数据库连接池获取数据库连接
//        try {
//            return dataSource.getConnection();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return null;

        Connection connection = null;
        try {
            Class<Driver> clazz = (Class<Driver>) Class.forName("com.mysql.jdbc.Driver");
            Driver driver = clazz.getConstructor().newInstance();

            Properties properties = new Properties();
            properties.put("user", "metadata");
            properties.put("password", "vkyKSdqYVlT7C3g6CVIF");
            connection = driver.connect("jdbc:mysql://183.61.12.83:3306/jcl?useUnicode=true&characterEncoding=utf8", properties);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return connection;
    }


    /**
     * 获取代理数据库连接
     *
     * @return
     */
    public static Connection getProxyConnection() {
        Connection connection = null;
        try {
            connection = DriverManager
                    .getConnection("jdbc:proxy_mysql://183.61.12.83:3306/jcl?"
                            + "user=metadata&password=vkyKSdqYVlT7C3g6CVIF&useUnicode=true&characterEncoding=UTF8");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return connection;
    }
}