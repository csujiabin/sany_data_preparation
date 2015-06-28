package config;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Configuration {
	
	private static Logger logger = Logger.getLogger(config.Configuration.class);
	
	/**
	 * ����Ĭ�ϵ����ݿ�ip��ַ����ͨ����ȡ�����ļ����ġ�
	 */
	public static String DATABASE_IP = "192.168.10.61";
	/**
	 * ����Ĭ�ϵ����ݿ�˿ڣ���ͨ����ȡ�����ļ����ġ�
	 */
	public static String DATABASE_PORT = "9170";
	
	/**
	 * ������Ҫ��ѯ�Ĺ���ID���ڵ��ļ�·����ͨ����ȡ�����ļ���ȡ��
	 * Ĭ��Ϊ�գ���ʾʹ��Ĭ�ϵĹ���ID�ļ�_paras.csv���������Cassandra������5364������ID��
	 * ÿ������ID��Ӧһ�����ݿ��cf_gk_xxx��
	 */
	public static String PARAS_ID_FILE_PATH = "";
	
	/**
	 * ��ȡ�����ļ�confg.xml�е����ݣ�������Ӧ�������
	 * @param filePath
	 */
	public static void loadConfig(String filePath) {
		try {
			// ��������ļ�config.xml�����ڣ���ʹ�������Ĭ�����á� 
			
			File configFile = new File(filePath);
			if (!configFile.exists()) {
				logger.info("config.xml doesn't exist, use default config");
				return;
			}
			
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document document = db.parse(filePath);

			// ��ȡ���ݿ�����ã�ip�Ͷ˿ںš�
			
			NodeList databaseNodes = document.getElementsByTagName("database");
			if (databaseNodes != null) {
				Node database = databaseNodes.item(0);
				NodeList children = database.getChildNodes();
				for (int i = 0; i < children.getLength(); i++) {
					Node child = children.item(i);
					if (child.getNodeType() == Node.ELEMENT_NODE && child.getNodeName().equals("ip")) {
						if (child.getFirstChild() != null) {
							DATABASE_IP = child.getFirstChild().getNodeValue().trim();
							logger.info("loaded database ip from config.xml: " + DATABASE_IP);
						}
					} else if (child.getNodeType() == Node.ELEMENT_NODE && child.getNodeName().equals("port")) {
						if (child.getFirstChild() != null) {
							DATABASE_PORT = child.getFirstChild().getNodeValue().trim();
							logger.info("loaded database port from config.xml: " + DATABASE_PORT);
						}
					}
				}
			}
			
			// ��ȡҪ��ѯ�Ĺ���ID���ڵ��ļ�·����
			
			NodeList parasNodes = document.getElementsByTagName("paras");
			if (parasNodes != null) {
				Node paras = parasNodes.item(0);
				NodeList children = paras.getChildNodes();
				for (int i = 0; i < children.getLength(); i++) {
					Node child = children.item(i);
					if (child.getNodeType() == Node.ELEMENT_NODE && child.getNodeName().equals("filepath")) {
						if (child.getFirstChild() != null) {
							PARAS_ID_FILE_PATH = child.getFirstChild().getNodeValue().trim();
							logger.info("loaded paras file path from config.xml: " + PARAS_ID_FILE_PATH);
						}
					}
				}
			}
			
		} catch (DOMException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
