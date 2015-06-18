package business;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.cql.jdbc.CassandraResultSet;

import util.DBConnector;
import util.DateTimeTransverter;

public class ParasDumper {
	
	/**
	 * 导出设备编号的默认文件
	 */
	private static final String EQUIPS_FILE_PATH = "equips.csv";
	/**
	 * 导出工况数据的默认文件夹
	 */
	private static final String PARAS_FOLDER_PATH = "paras\\";
	
	/** 
	 * 根据指定工况、设备类型，从数据库中导出设备编号到指定文件。
	 * @param parasID
	 * @param regex
	 * @param outputFilePath
	 */
	public void dumpEquips(String[] parasID, String regex, String outputFilePath) {
		try {
			Connection conn = DBConnector.getConnection();
			
			// 分别从指定工况表中查询所有指定设备编号（如所有泵车编号）
			HashSet<String> equipSet = new HashSet<String>();
			for (String paraID : parasID) {
				String cql = "select equip from cf_gk_" + paraID + " limit 1000000000;";
				Statement statement = conn.createStatement(); 
				ResultSet resultSet = statement.executeQuery(cql);
				
				while (resultSet.next()) {
					String equip = resultSet.getString("equip");
					if (equip.matches(regex)) {
						equipSet.add(equip);
					}
				}
				resultSet.close();
				statement.close();
			}
			System.out.println("[INFO] Queried " + equipSet.size() + " equips!"); //INFO
			
			// 把所有指定工况表中的所有设备编号的并集保存到文件
			OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(new File(outputFilePath)), "gbk");
			out.write("EquipID" + "\r\n");
			
			for (String equip : equipSet) {
				out.write(equip + "\r\n");
			}
			out.close();
			System.out.println("[INFO] Written file " + outputFilePath + "!"); //INFO
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/** 
	 * 根据指定工况、设备类型，从数据库中导出设备编号到默认文件。
	 * @param parasID
	 * @param regex
	 */
	public void dumpEquips(String[] parasID, String regex) {
		dumpEquips(parasID, regex, EQUIPS_FILE_PATH);
	}
	
	
	/** 
	 * 根据指定工况、设备类型、开始时间、结束时间，从数据库中导出工况数据到默认文件夹。
	 * @param paraID
	 * @param regex
	 * @param startTime
	 * @param endTime
	 */
	public void dumpParas(String[] parasID, String regex, String startTime, String endTime){
		if (startTime.length() <= 10) {
			startTime += " 00:00:00";
		}
		if (endTime.length() <= 10) {
			endTime += " 23:59:59";
		}
		
		long startTimeMilliseconds = DateTimeTransverter.getMilliseconds(startTime);
		long endTimeMilliseconds = DateTimeTransverter.getMilliseconds(endTime);
		
		try {
			// 从文件中读取设备列表，如果文件不存在，那么去Cassandra查询
			File equipsFile = new File(EQUIPS_FILE_PATH);
			if (!equipsFile.exists()) {
				dumpEquips(parasID, regex);
			}
			
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(EQUIPS_FILE_PATH), "gbk"));
			List<String> equipList = new ArrayList<String>();
			String line = "";
			while ((line = in.readLine()) != null) {
				String equip = line.trim();
				equipList.add(equip);
			}
			in.close();
			System.out.println("[INFO] Read equips from " + EQUIPS_FILE_PATH + "!"); //INFO
			
			// 把指定工况表中指定时间内所有指定设备（如所有泵车）的工况保存到文件
			File folder =new File(PARAS_FOLDER_PATH);
			if (!folder.exists() || !folder.isDirectory()) {
				folder.mkdir();
			}
			
			Connection conn = DBConnector.getConnection();
			for (String paraID : parasID) {
				String outputFilePath = PARAS_FOLDER_PATH + paraID + ".csv";
				OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(new File(outputFilePath)), "gbk");
				out.write("EquipID" + "," + "LocalTime" + "," + "ParaValue" + "\r\n");
				
				String cql = "select first 1000000000 " + startTimeMilliseconds + ".." + endTimeMilliseconds 
						+ " from cf_gk_" + paraID + " where equip in(?);";
				PreparedStatement stat = conn.prepareStatement(cql);
				
				for (String equip : equipList) {
					stat.setString(1, equip);
					CassandraResultSet cassandraResultSet = (CassandraResultSet) stat.executeQuery();
					
					while (cassandraResultSet.next()) {
						ResultSetMetaData metaData = cassandraResultSet.getMetaData();
						int columnCount = metaData.getColumnCount();
						for (int j = 1; j <= columnCount; j++) {
							String column1 = metaData.getColumnLabel(j);
							String value = cassandraResultSet.getString(j);
							
							String dateTime = DateTimeTransverter.parseDateTime(Long.parseLong(column1));
							out.write(equip + "," + dateTime + "," + value + "\r\n");
						}
					}
					cassandraResultSet.close();
				}
				
				out.close();
				stat.close();
				System.out.println("[INFO] Written para " + paraID + "!"); //INFO
			}
			System.out.println("[INFO] Written all paras to folder " + PARAS_FOLDER_PATH + "!"); //INFO
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	/** 
	 * 根据指定工况、开始时间、结束时间，从数据库中导出所有泵车工况数据到默认文件夹。
	 * @param paraID
	 * @param startTime
	 * @param endTime
	 */
	public void dumpBCParas(String[] parasID, String startTime, String endTime) {
		String regex = "[\\d]{2}BC[\\d]{8}";
		dumpParas(parasID, regex, startTime, endTime);
	}
	
}
