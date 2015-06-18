package test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeSet;

import model.Slice;

import org.apache.cassandra.cql.jdbc.CassandraResultSet;

import util.DBConnector;
import util.DateTimeTransverter;

public class TestForCQL2 {
	
	/**
	 * 查询工况表中的泵车有多少台
	 */
	public static void equipCountForPara() {
		String[] parasID = {"480", "481", "305", "869", "276", "277"};
		String regex = "[\\d]{2}BC[\\d]{8}";
		
		try {
			Connection conn = DBConnector.getConnection();
			
			HashSet<String> equipSet = new HashSet<String>();
			for (String paraID : parasID) {
				String cql = "select equip from cf_gk_" + paraID + " limit 1000000000;";
				PreparedStatement stat = conn.prepareStatement(cql);
				ResultSet resultSet = stat.executeQuery();
				
				int count = 0;
				while (resultSet.next()) {
					String equip = resultSet.getString("equip");
					if (equip.matches(regex)) {
						count++;
						equipSet.add(equip);
					}
				}
				System.out.println(paraID + ": " + count);//////
				
				resultSet.close();
				stat.close();
			}
			System.out.println("total distinct: " + equipSet.size());//////
			
			conn.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	/**
	 * 查看一台车的一个月的每个工况占多少内存
	 */
	public static void howBig() {
		long startTimeMilliseconds = DateTimeTransverter.getMilliseconds("2012-12-01 00:00:00");
		long endTimeMilliseconds = DateTimeTransverter.getMilliseconds("2012-12-31 23:59:59");
		String[] parasID = {"480", "481", "305", "869", "276", "277"};
		String regex = "[\\d]{2}BC[\\d]{8}";
		String equip = "12BC31870933";
		
		try {
			Connection conn = DBConnector.getConnection();
			
			String folderPath = "test_paras\\";
			File folder =new File(folderPath);
			if (!folder.exists() || !folder.isDirectory()) {
				folder.mkdir();
			}
			
			OutputStreamWriter out = null;
			Statement stat = null;
			CassandraResultSet cassandraResultSet = null;
			for (String paraID : parasID) {
				out = new OutputStreamWriter(new FileOutputStream(new File(folderPath + paraID + ".csv")), "gbk");
				out.write("LocalTime" + "," + "ParaValue" + "\r\n");
				
				String cql = "select first 1000000000 " + startTimeMilliseconds + ".." + endTimeMilliseconds + "from cf_gk_" + paraID + " where equip in('" + equip + "');";
				System.out.println(cql);//////
				stat = conn.createStatement();
				cassandraResultSet = (CassandraResultSet) stat.executeQuery(cql);
				
				while (cassandraResultSet.next()) {
					ResultSetMetaData metaData = cassandraResultSet.getMetaData();
					int columnCount = metaData.getColumnCount();
					for (int j = 1; j <= columnCount; j++) {
						String column1 = metaData.getColumnLabel(j);
						String value = cassandraResultSet.getString(j);
						
						String dateTime = DateTimeTransverter.parseDateTime(Long.parseLong(column1));
						out.write(dateTime + "," + value + "\r\n");
					}
				}
				cassandraResultSet.close();
				stat.close();
				out.close();
				
				System.out.println(paraID + ".csv written!");//////
			}	
			conn.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
		
		Connection conn = DBConnector.getConnection();
	}

}

class SliceComparator implements Comparator {

	@Override
	public int compare(Object o1, Object o2) {
		Slice s1 = (Slice) o1;
		Slice s2 = (Slice) o2;
		if (s1.startTime < s2.startTime) return -1;
		else if (s1.startTime > s2.startTime) return 1;
		else {
			if (s1.endTime < s2.endTime) return -1;
			else if (s1.endTime > s2.endTime) return 1;
			else return 0;
		}
	}
}
