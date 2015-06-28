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
	 * 程序默认的数据库ip地址，可通过读取配置文件更改。
	 */
	public static String DATABASE_IP = "192.168.10.61";
	/**
	 * 程序默认的数据库端口，可通过读取配置文件更改。
	 */
	public static String DATABASE_PORT = "9170";
	
	/**
	 * 程序需要查询的工况ID所在的文件路径，通过读取配置文件获取。
	 * 默认为空，表示使用默认的工况ID文件_paras.csv，里面包含Cassandra中所有5364个工况ID，
	 * 每个工况ID对应一个数据库表cf_gk_xxx。
	 */
	public static String PARAS_ID_FILE_PATH = "";
	
	/**
	 * 读取配置文件confg.xml中的内容，设置相应的配置项。
	 * @param filePath
	 */
	public static void loadConfig(String filePath) {
		try {
			// 如果配置文件config.xml不存在，则使用上面的默认配置。 
			
			File configFile = new File(filePath);
			if (!configFile.exists()) {
				logger.info("config.xml doesn't exist, use default config");
				return;
			}
			
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document document = db.parse(filePath);

			// 读取数据库的配置：ip和端口号。
			
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
			
			// 读取要查询的工况ID所在的文件路径。
			
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
