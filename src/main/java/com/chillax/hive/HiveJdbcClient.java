package com.chillax.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Iterator;

import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser.stringLiteralSequence_return;

public class HiveJdbcClient {

	public static void main(String[] args) {
		ResultSet rs = null;
		Connection conn = null;
		Statement stmt = null;
		try {
			Class.forName(HiveProperties.driverName);
			conn = DriverManager.getConnection(HiveProperties.url,
					HiveProperties.user, HiveProperties.password);
			stmt = conn.createStatement();
			String tableName = "testHiveJdbc";
//			String sql = "create table "+ tableName +"(id int,name string)";
			String sql = "select count(*) from stats_all";
			rs = stmt.executeQuery(sql);
			while(rs.next()){
				System.out.println(rs.getString(1));
				System.out.println(rs.getString(2));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
