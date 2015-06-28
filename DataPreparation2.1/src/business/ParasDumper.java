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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.cassandra.cql.jdbc.CassandraResultSet;
import org.apache.log4j.Logger;

import util.DBConnector;
import util.DateTimeTransverter;

public class ParasDumper {
	
	private static Logger logger = Logger.getLogger(business.ParasDumper.class);
	
	/**
	 * 根据工况ID文件中的工况ID，导出指定时间段内，设备文件中所有设备的工况数据，保存到输出文件夹中。
	 * @param inputParasIDFilePath
	 * @param inputEquipsFilePath
	 * @param startDateTime
	 * @param endDateTime
	 * @param outputParasFolderPath
	 */
	public void dumpParas(String inputParasIDFilePath,  String inputEquipsFilePath, 
			String startDateTime, String endDateTime, String outputParasFolderPath) {
		
		try {
			HashSet<String> paraIDSet = new HashSet<String>();
			
			// 如果没有指定工况ID文件的路径，则使用默认的工况ID文件_paras.csv。
			BufferedReader in = null;
			if (!inputParasIDFilePath.equals("")) {
				in = new BufferedReader(new InputStreamReader(new FileInputStream(inputParasIDFilePath), "gbk"));
				logger.info("read parasID from file: " + inputParasIDFilePath);
			} else {
				in = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/_paras.csv")));
				logger.info("read parasID from default file: _paras.csv");
			}
			
			String line = "";
			while ((line = in.readLine()) != null) {
				String paraID = line.trim();
				paraIDSet.add(paraID);
			}
			in.close();
			
			// 把所有工况ID放到一个数组中，然后调用对应的方法导出数据。
			String[] parasID = paraIDSet.toArray(new String[0]);
			dumpParas(parasID, inputEquipsFilePath, startDateTime, endDateTime, outputParasFolderPath);
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/** 
	 * 根据工况ID文件、设备号文件、开始时间、结束时间，从数据库中导出工况数据到文件夹。
	 * @param parasID
	 * @param inputEquipsFilePath
	 * @param startDateTime
	 * @param endDateTime
	 * @param outputParasFolderPath
	 */
	public void dumpParas(String[] parasID, String inputEquipsFilePath, 
			String startDateTime, String endDateTime, String outputParasFolderPath){
		
		if (startDateTime.length() <= 10) {
			startDateTime += " 00:00:00";
		}
		if (endDateTime.length() <= 10) {
			endDateTime += " 23:59:59";
		}
		
		long startTimeMilliseconds = DateTimeTransverter.getMilliseconds(startDateTime);
		long endTimeMilliseconds = DateTimeTransverter.getMilliseconds(endDateTime);
		
		try {
			File equipsFile = new File(inputEquipsFilePath);
			if (!equipsFile.exists()) {
				logger.error(inputEquipsFilePath + " does not exist");
				return;
			}
			
			// 读取设备文件中的设备号。
			
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(inputEquipsFilePath), "gbk"));
			List<String> equipList = new ArrayList<String>();
			String line = "";
			while ((line = in.readLine()) != null) {
				String equip = line.trim();
				equipList.add(equip);
			}
			in.close();
			
			logger.info("read equips from file: " + inputEquipsFilePath);
			
			
			// 如果输出文件夹中已经存在某些工况文件，那么就不必再重新去数据库查询，主要用于程序中断后重新运行。
			
			HashSet<String> existedParasSet = new HashSet<String>();
			
			File parasFolder =new File(outputParasFolderPath);
			if (parasFolder.exists()) {
				String[] paraFileNames = parasFolder.list();
				for (String fileName : paraFileNames) {
					String paraID = fileName.split("\\.")[0];
					existedParasSet.add(paraID);
				}
			} else {
				parasFolder.mkdir();
			}
			
			// 从数据库中导出每个工况的数据。
			
			Connection conn = DBConnector.getConnection();
			for (String paraID : parasID) {
				
				if (existedParasSet.contains(paraID)) continue;
				
				String outputParaFilePath = outputParasFolderPath + paraID + ".csv";
				OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(new File(outputParaFilePath)), "gbk");
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
					
					logger.debug("queried para and written into file: paraID=" + paraID + " equip=" + equip);
				}
				
				out.close();
				stat.close();
				
				logger.info("queried para and written into file: paraID=" + paraID);
			}
			
			// 导完工况数据后不再使用数据库连接，关闭连接。
			conn.close();
			
			logger.info("queried all paras and written into folder " + outputParasFolderPath);
			
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
