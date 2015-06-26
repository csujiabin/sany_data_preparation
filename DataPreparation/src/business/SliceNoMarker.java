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
import java.util.HashMap;
import java.util.TreeMap;

import util.DateTimeTransverter;
import model.Slice;

public class SliceNoMarker {

	/**
	 * 默认输入切片文件路径
	 */
	private static final String SLICE_FILE_PATH = "slice_2012-12.csv";
	/**
	 * 默认输入工况数据文件夹路径
	 */
	private static final String PARAS_FOLDER_PATH = "paras_2012-12\\";
	/**
	 * 标记了切片号的输出工况数据文件夹路径
	 */
	private static final String MARKED_PARAS_FOLDER_PATH = "marked_paras_2012-12\\";
	
	
	/**
	 * 根据默认切片数据文件对默认文件夹中的工况数据进行切片号标记，并输出到默认文件夹。
	 */
	public void markSliceNo() {
		HashMap<String, TreeMap<Integer, Slice>> equipSliceMap = new HashMap<>();
		
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(SLICE_FILE_PATH), "gbk"));
			TreeMap<Integer, Slice> sliceTreeMap = null;
			String line = in.readLine(); //表头
			while ((line = in.readLine()) != null) {
				String[] columns = line.split(",");
				String equip = columns[0];
				Integer sliceNo = Integer.parseInt(columns[1]);
				String startTime = columns[2];
				String endTime = columns[3];
				
				Slice slice = new Slice(DateTimeTransverter.getMilliseconds(startTime), DateTimeTransverter.getMilliseconds(endTime));
				
				if (equipSliceMap.containsKey(equip)) {
					sliceTreeMap = equipSliceMap.get(equip);
				} else {
					sliceTreeMap = new TreeMap<>();
					equipSliceMap.put(equip, sliceTreeMap);
				}
				
				sliceTreeMap.put(sliceNo, slice);
			}
			in.close();
			System.out.println("[INFO] equipSliceMap.size(): " + equipSliceMap.size() + "!"); //INFO
			System.out.println("[INFO] Read slices from " + SLICE_FILE_PATH + "!"); //INFO
			
			
			File parasFolder = new File(PARAS_FOLDER_PATH);
			if (!parasFolder.exists() || !parasFolder.isDirectory()) return;
			
			File markedParaFolder =new File(MARKED_PARAS_FOLDER_PATH);
			if (!markedParaFolder.exists() || !markedParaFolder.isDirectory()) {
				markedParaFolder.mkdir();
			}
			
			OutputStreamWriter out = null;
			File[] paraFiles = parasFolder.listFiles();
			for (File paraFile: paraFiles) {
				String paraID = paraFile.getName().split("\\.")[0];
				
				in = new BufferedReader(new InputStreamReader(new FileInputStream(paraFile), "gbk"));
				String outputFilePath = MARKED_PARAS_FOLDER_PATH + "marked_" + paraFile.getName();
				out = new OutputStreamWriter(new FileOutputStream(new File(outputFilePath)), "gbk");
				out.write("ParaID" + "," + "EquipID" + "," + "SliceNo" + "," + "LocalTime" + "," + "ParaValue" + "\r\n");
				
				in.readLine(); //表头
				while ((line = in.readLine()) != null) {
					String[] columns = line.split(",");
					String equip = columns[0];
					String localTime = columns[1];
					String paraValue = columns[2];
					
					int sliceNo = getSliceNo(equipSliceMap.get(equip), DateTimeTransverter.getMilliseconds(localTime));
					out.write(paraID + "," + equip + "," + sliceNo + "," + localTime + "," + paraValue + "\r\n");
//					System.out.println("[INFO] Written: " 
//							+ paraID + "," + equip + "," + sliceNo + "," + localTime + "," + paraValue); //INFO
				}
				
				out.close();
				in.close();
				System.out.println("[INFO] Read para from " + paraFile.getName() 
						+ " and written marked para into marked_" + paraFile.getName() + "!"); //INFO
			}
			System.out.println("[INFO] Written marked paras into " + MARKED_PARAS_FOLDER_PATH + "!"); //INFO
			
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 根据时间戳获取工况值得切片号，其中一台车的所有切片存放在一个TreeMap中。
	 * @param sliceMap
	 * @param timeStamp
	 * @return
	 */
	public int getSliceNo(TreeMap<Integer, Slice> sliceMap, long timeStamp) {
		if (sliceMap == null) return -1;
		
		int left = 1, right = sliceMap.size(); //sliceNo从1开始
		while (left <= right) {
			int mid = (left + right) / 2;
			Slice slice = sliceMap.get(mid);
			
			if (slice.startTime > timeStamp) {
				right = mid - 1;
				continue;
			}
			if (slice.endTime < timeStamp) {
				left = mid + 1;
				continue;
			}
			return mid;
		}
		return -2;
	}
	

//	public static void main(String[] args) {
//		// 根据切片文件对工况文件添加切片号
//		SliceNoMarker sliceNoMarker = new SliceNoMarker();
//		
//		long timerStart = DateTimeTransverter.getCurrentMilliseconds();
//		sliceNoMarker.markSliceNo();
//		long timerEnd = DateTimeTransverter.getCurrentMilliseconds();
//		System.out.println("[INFO] markSliceNo() consumed time: " + (timerEnd - timerStart)/1000 + "s!"); //INFO
//		
//		System.out.println("[INFO] Program quit!"); //INFO
//	}
}
