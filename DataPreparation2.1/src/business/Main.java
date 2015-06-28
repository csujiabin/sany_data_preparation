package business;

import java.io.File;

import org.apache.log4j.Logger;

import config.Configuration;

public class Main {

	private static Logger logger = Logger.getLogger(business.Main.class);
	
	/**
	 * 整个程序的入口
	 * @param args
	 */
	public static void main(String[] args) {
		
		logger.info("************************************************************");
		logger.info("program begins...");
		
		// 运行该程序时需指定四个参数，分别表示查询的开始日期与时间，结束日期与时间；
		// 也可只指定两个参数，即只指定开始日期和结束日期，则开始时间为开始日期的00:00:00，结束时间为结束日期的23:59:59。
		
		if (args.length != 2 && args.length != 4) {
	    	System.err.println("Usage: data_preparation.jar <start_date_time> <end_date_time>");
	    	System.err.println("e.g. data_preparation.jar yyyy-MM-dd HH:mm:ss yyyy-MM-dd HH:mm:ss");
	    	System.err.println("e.g. data_preparation.jar yyyy-MM-dd yyyy-MM-dd");
	    	System.exit(1);
	    }
		
		String startDateTime = args[0];
		String endDateTime = args[1];
		
		if (args.length == 4) {
			startDateTime = args[0] + " " + args[1];
			endDateTime = args[2] + " " + args[3];
		}
		
		logger.info("query startDateTime: " + startDateTime);
		logger.info("query endDateTime: " + endDateTime);
		
		// 加载配置文件config.xml，该程序有两处可配置的地方，如果在当前目录找不到config.xml，则程序使用默认配置：
		// 1. 数据库的ip地址和端口号（默认配置为192.168.10.61）；
		// 2. 要查询的工况ID文件路径（默认文查询所有工况，即Cassandra中所有以cf_gk_开头的5364张表）。
		// 程序如果找不到指定的工况ID文件，也会查询所有工况。
		
		Configuration.loadConfig("config.xml");
		
		// 程序的第一个阶段：首先从划分切片所依据的6个工况表中查询所有的泵车设备号。
		// 结果保存在equips.csv中，如果程序运行前该文件已存在，则会跳过该阶段。
		
		String equipsFilePath = "equips.csv";
		File equipsFile = new File(equipsFilePath);
		if (!equipsFile.exists() || !equipsFile.isFile()) {
			logger.info("equip.csv doesn't exist, begin to dump equips...");
			new EquipsDumper().dumpEquips(equipsFilePath);
		}
		
		// 程序的第二个阶段：查询上面指定时间段内，equip.csv中所有设备的划分切片所依据的6个工况，
		// 然后先对每个工况以7.4小时进行切分，再把6个工况分别得到的切片进行合并，得到最终的切片。
		// 保存在slices.csv中，如果程序运行前该文件已经存在，则会跳过该阶段。
		
		String slicesFilePath = "slices.csv";
		File slicesFile = new File(slicesFilePath);
		if (!slicesFile.exists() || !slicesFile.isFile()) {
			logger.info("slice.csv doesn't exist, begin to generate slices...");
			new ParasSlicer().generateSlices(equipsFilePath, startDateTime, endDateTime, slicesFilePath);
		}
		
		// 根据配置文件config.xml中指定的工况ID文件，从数据库中导出上面指定时间段内的相应的工况数据。
		// 如果没有配置工况ID文件的路径，则使用默认值，即导出数据库中所有工况数据。
		
		// 工况ID文件路径不为空，表示有配置工况ID文件，检查该文件是否存在。
		if (!Configuration.PARAS_ID_FILE_PATH.equals("")) {
			File parasIDFile = new File(Configuration.PARAS_ID_FILE_PATH);
			if (!parasIDFile.exists() || !parasIDFile.isFile()) {
				logger.error("paras ID file doesn't exist: " + Configuration.PARAS_ID_FILE_PATH);
				System.exit(1);
			}
		}
		
		String parasFolderPath = "paras/";
		new ParasDumper().dumpParas(Configuration.PARAS_ID_FILE_PATH, equipsFilePath, 
									startDateTime, endDateTime, parasFolderPath);
		
		// 根据切片文件，对工况文件夹中的每一个工况文件进行切片号标记，并输出到另外文件夹中。
		
		String markedParasFolderPath = "marked_paras/";
		new SliceNoMarker().markSliceNo(parasFolderPath, slicesFilePath, markedParasFolderPath);
		
		logger.info("Program quit");
		logger.info("************************************************************");
	}
}
