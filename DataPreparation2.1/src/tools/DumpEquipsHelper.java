package tools;

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
import java.util.HashSet;

import org.apache.log4j.Logger;

public class DumpEquipsHelper {
	private static Logger logger = Logger.getLogger(tools.DumpEquipsHelper.class);
	
	private static String CONN_STRING;

	/** 
	 * 根据指定工况、设备类型，从数据库中导出设备编号到指定文件。
	 * @param parasID
	 * @param regex
	 * @param outputFilePath
	 */
	public void dumpEquips(String[] parasID, String regex, String outputFilePath) {
		try {
			Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
			Connection conn = DriverManager.getConnection(CONN_STRING);
			logger.info("Connected to Cassandra");
			
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
				logger.info("Queried cf_gk_" + paraID);
			}
			logger.info("Queried " + equipSet.size() + " equips");
			
			// 把所有指定工况表中的所有设备编号的并集保存到文件
			OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(new File(outputFilePath)), "gbk");
			out.write("EquipID" + "\r\n");
			
			for (String equip : equipSet) {
				out.write(equip + "\r\n");
			}
			out.close();
			conn.close();
			logger.info("Written equips into file " + outputFilePath);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 该文件可单独用于导出所有泵车设备号，可导成jar包放于服务器上执行，输出的设备号可作为MapReduce程序的输入。
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
	    	System.err.println("Usage: dump_equips.jar <database_ip> <database_port>");
	    	System.exit(1);
	    }
		
		CONN_STRING = String.format("jdbc:cassandra://%s:%s/sany_test_single?version=2.0.0", args[0], args[1]);
		
		String[] parasID = {"480", "481", "305", "869", "276", "277"};
		String regex = "[\\d]{2}BC[\\d]{8}";
		String outputFilePath = "equips.csv";
		new DumpEquipsHelper().dumpEquips(parasID, regex, outputFilePath);
		
		logger.info("Program quit normally");
	}
}
