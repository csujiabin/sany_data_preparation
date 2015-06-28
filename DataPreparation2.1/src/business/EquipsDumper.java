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
	 * 根据泵车划分切片所依据的6个工况表，导出所有泵车设备号到指定的文件中。
	 * @param outputFilePath
	 */
	public void dumpEquips(String outputFilePath) {
		String[] parasID = {"480", "481", "305", "869", "276", "277"};
		String regex = "[\\d]{2}BC[\\d]{8}";
		dumpEquips(parasID, regex, outputFilePath);
	}
	
	
	/** 
	 * 根据指定工况ID、设备类型，从数据库中导出设备编号到指定文件。
	 * @param parasID
	 * @param regex
	 * @param outputFilePath
	 */
	public void dumpEquips(String[] parasID, String regex, String outputFilePath) {
		try {
			// 从指定工况表中查询所有设备号，保存到一个HashSet中。 
			
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
			
			// 把设备号写入到文件。
			
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
