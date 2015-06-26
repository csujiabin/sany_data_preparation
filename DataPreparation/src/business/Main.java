package business;

import util.DateTimeTransverter;

public class Main {

	public static void main(String[] args) {
//		// 从划分切片所依据的六个工况表中导出设备列表，并导出这六个工况表
//		ParasDumper parasDumper = new ParasDumper();
//		
//		String[] parasID = {"480", "481", "305", "869", "276", "277"};
//		String startTime = "2012-12-01 00:00:00";
//		String endTime = "2012-12-31 23:59:59";
//		String[] startTimes = {"2012-10-01", "2012-11-01", "2012-12-01", "2013-01-01", 
//				"2013-02-01", "2013-03-01", "2013-04-01", "2013-05-01", "2013-06-01"};
//		String[] endTimes = {"2012-10-31", "2012-11-30", "2012-12-31", "2013-01-31", 
//				"2013-02-28", "2013-03-31", "2013-04-30", "2013-05-31", "2013-06-30"};
//		
//		for (int i = 0; i < startTimes.length; i++) {
//		
//			long timerStart = DateTimeTransverter.getCurrentMilliseconds();
//			parasDumper.dumpBCParas(parasID, startTimes[i], endTimes[i]);
//			long timerEnd = DateTimeTransverter.getCurrentMilliseconds();
//			System.out.println("[INFO] dumpBCParas() consumed time: " + (timerEnd - timerStart)/1000 + "s!"); //INFO
//			
//			
//			// 分别把六个工况表进行切片处理
//			ParasSlicer parasSlicer = new ParasSlicer();
//			
//			timerStart = DateTimeTransverter.getCurrentMilliseconds();
//			parasSlicer.sliceParas();
//			timerEnd = DateTimeTransverter.getCurrentMilliseconds();
//			System.out.println("[INFO] sliceParas() consumed time: " + (timerEnd - timerStart)/1000 + "s!"); //INFO
//			
//			// 合并六个工况的切片
//			timerStart = DateTimeTransverter.getCurrentMilliseconds();
//			parasSlicer.mergeSlices("slice_" + startTimes[i].substring(0, 7) + ".csv");
//			timerEnd = DateTimeTransverter.getCurrentMilliseconds();
//			System.out.println("[INFO] mergeSlices() consumed time: " + (timerEnd - timerStart)/1000 + "s!"); //INFO
//			
//		}
		
		
//		// 根据工况号导出工况数据
//		String[] parasID = {"272", "273", "276", "277"}; // 发动机转速（转/分），分动箱转速（转/分），泵送排量（%），换向次数
		String[] parasID = {"183", "205", "207", "216"}; //紧急停止，高压启动，水泵开关，支腿收回
		String startTime = "2012-12-01 00:00:00";
		String endTime = "2012-12-31 23:59:59";
		
		ParasDumper parasDumper = new ParasDumper();
		long timerStart = DateTimeTransverter.getCurrentMilliseconds();
		parasDumper.dumpBCParas(parasID, startTime, endTime);
		long timerEnd = DateTimeTransverter.getCurrentMilliseconds();
		System.out.println("[INFO] dumpBCParas() consumed time: " + (timerEnd - timerStart)/1000 + "s!"); //INFO

		
//		// 根据切片文件对工况文件添加切片号
//		SliceNoMarker sliceNoMarker = new SliceNoMarker();
//		long timerStart = DateTimeTransverter.getCurrentMilliseconds();
//		sliceNoMarker.markSliceNo();
//		long timerEnd = DateTimeTransverter.getCurrentMilliseconds();
//		System.out.println("[INFO] markSliceNo() consumed time: " + (timerEnd - timerStart)/1000 + "s!"); //INFO
		
		System.out.println("[INFO] Program quit!"); //INFO
	}

}
