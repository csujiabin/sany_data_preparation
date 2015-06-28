package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import config.Configuration;

public class DBConnector {
	
	private static Logger logger = Logger.getLogger(util.DBConnector.class);
	
	private static Connection conn = null;
	
	private DBConnector() {
		
	}
	
	/**
	 * 单例模式返回一个到Cassandra数据库的连接，cql版本为2.0.0
	 * @return
	 */
	public static Connection getConnection() {
		if (conn == null) {
			try {
				Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
				String connStr = String.format("jdbc:cassandra://%s:%s/sany_test_single?version=2.0.0", 
						Configuration.DATABASE_IP, Configuration.DATABASE_PORT);
				conn = DriverManager.getConnection(connStr);
				
				logger.info("connnected to Cassandra: " + connStr);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return conn;
	}
}
