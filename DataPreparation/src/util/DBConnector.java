package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnector {
	private static Connection conn = null;
	
	private DBConnector() {
		
	}
	
	/**
	 * ����ģʽ����һ����Cassandra���ݿ�����ӣ�cql�汾Ϊ2.0.0
	 * @return
	 */
	public static Connection getConnection() {
		if (conn == null) {
			try {
				Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
				conn = DriverManager.getConnection("jdbc:cassandra://192.168.10.61:9170/sany_test_single?version=2.0.0");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return conn;
	}
}
