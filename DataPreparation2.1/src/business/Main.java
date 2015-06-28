package business;

import java.io.File;

import org.apache.log4j.Logger;

import config.Configuration;

public class Main {

	private static Logger logger = Logger.getLogger(business.Main.class);
	
	/**
	 * ������������
	 * @param args
	 */
	public static void main(String[] args) {
		
		logger.info("************************************************************");
		logger.info("program begins...");
		
		// ���иó���ʱ��ָ���ĸ��������ֱ��ʾ��ѯ�Ŀ�ʼ������ʱ�䣬����������ʱ�䣻
		// Ҳ��ָֻ��������������ָֻ����ʼ���ںͽ������ڣ���ʼʱ��Ϊ��ʼ���ڵ�00:00:00������ʱ��Ϊ�������ڵ�23:59:59��
		
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
		
		// ���������ļ�config.xml���ó��������������õĵط�������ڵ�ǰĿ¼�Ҳ���config.xml�������ʹ��Ĭ�����ã�
		// 1. ���ݿ��ip��ַ�Ͷ˿ںţ�Ĭ������Ϊ192.168.10.61����
		// 2. Ҫ��ѯ�Ĺ���ID�ļ�·����Ĭ���Ĳ�ѯ���й�������Cassandra��������cf_gk_��ͷ��5364�ű���
		// ��������Ҳ���ָ���Ĺ���ID�ļ���Ҳ���ѯ���й�����
		
		Configuration.loadConfig("config.xml");
		
		// ����ĵ�һ���׶Σ����ȴӻ�����Ƭ�����ݵ�6���������в�ѯ���еıó��豸�š�
		// ���������equips.csv�У������������ǰ���ļ��Ѵ��ڣ���������ý׶Ρ�
		
		String equipsFilePath = "equips.csv";
		File equipsFile = new File(equipsFilePath);
		if (!equipsFile.exists() || !equipsFile.isFile()) {
			logger.info("equip.csv doesn't exist, begin to dump equips...");
			new EquipsDumper().dumpEquips(equipsFilePath);
		}
		
		// ����ĵڶ����׶Σ���ѯ����ָ��ʱ����ڣ�equip.csv�������豸�Ļ�����Ƭ�����ݵ�6��������
		// Ȼ���ȶ�ÿ��������7.4Сʱ�����з֣��ٰ�6�������ֱ�õ�����Ƭ���кϲ����õ����յ���Ƭ��
		// ������slices.csv�У������������ǰ���ļ��Ѿ����ڣ���������ý׶Ρ�
		
		String slicesFilePath = "slices.csv";
		File slicesFile = new File(slicesFilePath);
		if (!slicesFile.exists() || !slicesFile.isFile()) {
			logger.info("slice.csv doesn't exist, begin to generate slices...");
			new ParasSlicer().generateSlices(equipsFilePath, startDateTime, endDateTime, slicesFilePath);
		}
		
		// ���������ļ�config.xml��ָ���Ĺ���ID�ļ��������ݿ��е�������ָ��ʱ����ڵ���Ӧ�Ĺ������ݡ�
		// ���û�����ù���ID�ļ���·������ʹ��Ĭ��ֵ�����������ݿ������й������ݡ�
		
		// ����ID�ļ�·����Ϊ�գ���ʾ�����ù���ID�ļ��������ļ��Ƿ���ڡ�
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
		
		// ������Ƭ�ļ����Թ����ļ����е�ÿһ�������ļ�������Ƭ�ű�ǣ�������������ļ����С�
		
		String markedParasFolderPath = "marked_paras/";
		new SliceNoMarker().markSliceNo(parasFolderPath, slicesFilePath, markedParasFolderPath);
		
		logger.info("Program quit");
		logger.info("************************************************************");
	}
}
