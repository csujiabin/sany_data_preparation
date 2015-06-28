package business;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;

import org.apache.log4j.Logger;

import util.DBConnector;

public class EquipsDumper {

	private static Logger logger = Logger.getLogger(business.EquipsDumper.class);
	
	/**
	 * ���ݱó�������Ƭ�����ݵ�6���������������бó��豸�ŵ�ָ�����ļ��С�
	 * @param outputFilePath
	 */
	public void dumpEquips(String outputFilePath) {
		String[] parasID = {"480", "481", "305", "869", "276", "277"};
		String regex = "[\\d]{2}BC[\\d]{8}";
		dumpEquips(parasID, regex, outputFilePath);
	}
	
	
	/** 
	 * ����ָ������ID���豸���ͣ������ݿ��е����豸��ŵ�ָ���ļ���
	 * @param parasID
	 * @param regex
	 * @param outputFilePath
	 */
	public void dumpEquips(String[] parasID, String regex, String outputFilePath) {
		try {
			// ��ָ���������в�ѯ�����豸�ţ����浽һ��HashSet�С� 
			
			Connection conn = DBConnector.getConnection();
			
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
				
				logger.info("queried equips from cf_gk_" + paraID);
			}
			
			logger.info("totally queried equips numbers: " + equipSet.size());
			
			// ���豸��д�뵽�ļ���
			
			OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(new File(outputFilePath)), "gbk");
			for (String equip : equipSet) {
				out.write(equip + "\r\n");
			}
			out.close();

			logger.info("written eqiups into file: " + outputFilePath);
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
