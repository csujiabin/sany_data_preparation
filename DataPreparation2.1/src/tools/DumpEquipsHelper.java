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
	 * ����ָ���������豸���ͣ������ݿ��е����豸��ŵ�ָ���ļ���
	 * @param parasID
	 * @param regex
	 * @param outputFilePath
	 */
	public void dumpEquips(String[] parasID, String regex, String outputFilePath) {
		try {
			Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
			Connection conn = DriverManager.getConnection(CONN_STRING);
			logger.info("Connected to Cassandra");
			
			// �ֱ��ָ���������в�ѯ����ָ���豸��ţ������бó���ţ�
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
			
			// ������ָ���������е������豸��ŵĲ������浽�ļ�
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
	 * ���ļ��ɵ������ڵ������бó��豸�ţ��ɵ���jar�����ڷ�������ִ�У�������豸�ſ���ΪMapReduce��������롣
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
