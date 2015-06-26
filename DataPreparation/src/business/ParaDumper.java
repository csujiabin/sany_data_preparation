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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeMap;

import org.apache.cassandra.cql.jdbc.CassandraResultSet;

import util.DBConnector;

public class ParaDumper {
	
	/**
	 * 根据指定设备号和工况号从指定数据库表（如max，min）中导出数据到文件<equip, sliceNo, value>
	 * @param table
	 * @param equip
	 * @param paraID
	 * @param outputFilePath
	 */
	public void dumpPara(String table, String equip, String paraID, String outputFilePath) {
		Connection conn = DBConnector.getConnection();
			
		try {
			OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(new File(outputFilePath)), "gbk");
			out.write("EquipID" + "," + "SliceNo" + "," + "Value" + "\r\n");
			
			String cql = "select first 1000000000 * from " + table + " where equip='" + equip + "';";
			Statement statement = conn.createStatement(); 
			CassandraResultSet cassandraResultSet = (CassandraResultSet) statement.executeQuery(cql);
			
			while (cassandraResultSet.next()) {
				ResultSetMetaData metaData = cassandraResultSet.getMetaData();
				int columnCount = metaData.getColumnCount();
				System.out.println("[INFO] ReslutSet column count: " + columnCount); //INFO
				
				for (int j = 1; j <= columnCount; j++) {
					String column1 = metaData.getColumnLabel(j);
					String value = cassandraResultSet.getString(j);
					
					String[] tmp = column1.split("_");
					if (!tmp[0].equals(paraID)) continue;
					
					String sliceNo = tmp[1];
					out.write(equip + "," + sliceNo + "," + value + "\r\n");
					System.out.println("[WRITE] " + equip + "," + sliceNo + "," + value); //INFO
				}
			}
			cassandraResultSet.close();
			statement.close();
			out.close();
			System.out.println("[INFO] Written reslut into flie " + outputFilePath); //INFO
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	class SliceInfo {
		String max;
		String min;
		String average;
		String standard;
	}
	
	
	/**
	 * 把max，min，average，standard四个文件<equip, sliceNo, value>合并到一个文件<equip, sliceNo, max, min, average, standard>
	 * @param equip
	 * @param paraID
	 * @param inputFolderPath
	 * @param outputFilePath
	 */
	public void mergeSliceInfo(String equip, String paraID, String inputFolderPath, String outputFilePath) {
		try {
			File inputFolder = new File(inputFolderPath);
			if (!inputFolder.exists() || !inputFolder.isDirectory()) return;
			
			TreeMap<Integer, SliceInfo> sliceTreeMap = new TreeMap<>();
			
			BufferedReader in = null;
			File[] inputFiles = inputFolder.listFiles();
			for (File file : inputFiles) {
				String column = file.getName().split("\\.")[0];
				
				in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "gbk"));
				String line = in.readLine();
				while ((line = in.readLine()) != null) {
					String[] tmp = line.split(",");
					Integer sliceNo = Integer.parseInt(tmp[1]);
					String value = tmp[2];
					
					SliceInfo sliceInfo = null;
					if (sliceTreeMap.containsKey(sliceNo)) {
						sliceInfo = sliceTreeMap.get(sliceNo);
					} else {
						sliceInfo = new SliceInfo();
						sliceTreeMap.put(sliceNo, sliceInfo);
					}
					
					if ("max".equals(column)) {
						sliceInfo.max = value;
					} else if ("min".equals(column)) {
						sliceInfo.min = value;
					} else if ("average".equals(column)) {
						sliceInfo.average = value;
					} else if ("standard".equals(column)) {
						sliceInfo.standard = value;
					}
				}
				in.close();
				System.out.println("[INFO] Read file " + file.getName()); //INFO
			}
			System.out.println("[INFO] Read all files in folder " + inputFolderPath); //INFO
			
			
			OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(new File(outputFilePath)), "gbk");
			out.write("equip" + "," + "para_id" + "," + "slice_no" + "," 
					+ "max" + "," + "min" + "," + "average" + "," + "standard" + "\r\n");

			for (Integer sliceNo : sliceTreeMap.keySet()) {
				SliceInfo sliceInfo = sliceTreeMap.get(sliceNo);
				out.write(equip + "," + paraID + "," + sliceNo + "," + sliceInfo.max + "," + sliceInfo.min + "," 
						+ sliceInfo.average + "," + sliceInfo.standard + "\r\n");;
			}
			out.close();
			System.out.println("[INFO] Written reslut into file " + outputFilePath); //INFO
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	/**
	 * 2015-06-19
	 * 应刘老师需求，从max,min,average,standard四个表中导出分别导出两台车的数据，并合并到一个文件。
	 * @param args
	 */
	public static void main(String[] args) {
		ParaDumper paraDumper = new ParaDumper();
		
		// 从几个数据库表里导出数据到csv文件 <equip, sliceNo, value>，每台车放到一个文件夹中
		String paraID = "1483";
		
		String[] tables = {"max", "min", "average", "standard"};
		String equip = "11BC53130030";
		
		File folder = new File(equip);
		if (!folder.exists()) {
			folder.mkdir();
		}
		
		for (String table : tables) {
			paraDumper.dumpPara(table, equip, paraID, equip + "\\" + table + ".csv");
		}
		
		equip = "11BC53135752";
		folder = new File(equip);
		if (!folder.exists()) {
			folder.mkdir();
		}
		for (String table : tables) {
			paraDumper.dumpPara(table, equip, paraID, equip + "\\" + table + ".csv");
		}
		
		
		// 把几csv文件合并到一个文件 <equip, paraID, sliceNo, value1, value2, ...>
		equip = "11BC53130030";
		paraDumper.mergeSliceInfo(equip, paraID, equip, equip + "_" + paraID + ".csv");
		
		equip = "11BC53135752";
		paraDumper.mergeSliceInfo(equip, paraID, equip, equip + "_" + paraID + ".csv");
		
		System.out.println("[INFO] Program quit!"); //INFO
	}

}
