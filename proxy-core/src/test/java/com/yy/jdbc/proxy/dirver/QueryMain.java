package com.yy.jdbc.proxy.dirver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.StringUtils;

public class QueryMain {

	public static void main(String[] args) throws SQLException, IOException {
		Connection conn = DriverManager
				.getConnection("jdbc:proxy_mysql://183.61.12.83:3306/jcl?"
						+ "user=metadata&password=vkyKSdqYVlT7C3g6CVIF&useUnicode=true&characterEncoding=UTF8");
		Statement stmt = conn.createStatement();
		stmt.execute("set auto_rewrite=on");
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					System.in));
			StringBuilder sql = new StringBuilder();
			System.out.println("input sql:");
			while (true) {

				String line = reader.readLine();
				if (line == null) {
					break;
				}
				if (StringUtils.equalsIgnoreCase(line, "quit")) {
					break;
				}
				if (StringUtils.endsWith(line, ";")) {

				} else {
					sql.append(line).append("\r\n");
					continue;
				}
				boolean isSet = StringUtils.startsWithIgnoreCase(sql, "set ");
				boolean isCreate = StringUtils.startsWithIgnoreCase(sql,
						"create m");
				boolean isDrop = StringUtils
						.startsWithIgnoreCase(sql, "drop m");

				try {
					if (isSet || isCreate || isDrop) {
						stmt.execute(sql.toString());
					} else {
						ResultSet rs = stmt.executeQuery(sql.toString());
						ResultSetMetaData meta = rs.getMetaData();
						int columnCount = meta.getColumnCount();
						for (int i = 1; i <= columnCount; i++) {
							System.out.print(meta.getColumnLabel(i));
							System.out.print("\t");
						}
						System.out.println();
						while (rs.next()) {
							for (int i = 1; i <= columnCount; i++) {
								System.out.print(rs.getString(i));
								System.out.print("\t");
							}
							System.out.println();
						}
						rs.close();
					}
					System.out.println("input sql:");
				} catch (Exception e) {
					e.printStackTrace();
					continue;
				} finally {
					sql = new StringBuilder();
				}
			}
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}

}
