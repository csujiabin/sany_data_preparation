package test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class TestConnectionToCassandra {
	
	/**
	 * 根据指定设备号和工况号从指定数据库表（如max，min）中导出数据到文件<equip, sliceNo, value>
	 * @param table
	 * @param equip
	 * @param paraID
	 * @param outputFilePath
	 */
	public void dumpPara(String table, String equip, String paraID, String outputFilePath) {
					
		try {
			Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
			Connection conn = DriverManager.getConnection("jdbc:cassandra://192.168.10.61:9170/sany_test_single?version=3.0.0");

			OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(new File(outputFilePath)), "gbk");
			out.write("EquipID" + "," + "SliceNo" + "," + "Value" + "\r\n");
			
			String cql = "select * from " + table + " where equip='" + equip + "';";
			Statement statement = conn.createStatement(); 
			ResultSet resultSet = statement.executeQuery(cql);
			
			while (resultSet.next()) {
				String column1 = resultSet.getString("column1");
				String value = resultSet.getString("value");
				
				String[] tmp = column1.split("_");
				if (!tmp[0].equals(paraID)) continue;
				
				String sliceNo = tmp[1];
				out.write(equip + "," + sliceNo + "," + value + "\r\n");
				System.out.println("[WRITE] " + equip + "," + sliceNo + "," + value); //INFO
			}
			resultSet.close();
			statement.close();
			out.close();
			conn.close();
			
			System.out.println("[INFO] Written reslut into flie " + outputFilePath); //INFO
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		TestConnectionToCassandra instance = new TestConnectionToCassandra();
		instance.dumpPara("max", "11BC53130030", "1483", "11BC53130030_1483_max.csv");
		System.out.println("[INFO] Program quit!"); //INFO
	}

}
