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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import model.Slice;
import util.DateTimeTransverter;

public class ParasSlicer {
	
	/**
	 * 切片算法所用的7.4小时的毫秒数
	 */
	private static final long SLICE_SPACING_MILLISECOND = (long) (7.4 * 3600 * 1000);
	
	/**
	 * 要进行切片的单工况文件所在的文件夹
	 */
	private static final String PARAS_FOLDER_PATH = "paras\\";
	/**
	 * 对每个单工况文件切片后的切片文件所在的文件夹
	 */
	private static final String SLICES_FOLDER_PATH = "slices\\";
	/**
	 * 把多个单工况切片文件合并后的最红切片文件
	 */
	private static final String FINAL_SLICE_FEIL_PATH = "slice.csv";
	
	/**
	 * 从文件中读取单工况数据，划分切片并保存到文件。
	 * @param inputParaFilePath
	 * @param outputSliceFilePath
	 */
	public void slicePara(String inputParaFilePath, String outputSliceFilePath) {
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(inputParaFilePath), "gbk"));
			OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(new File(outputSliceFilePath)), "gbk");
			out.write("EquipID" + "," + "SliceNo" + "," + "StartTime" + "," + "EndTime" + "\r\n");
			
			String currentEquip = "";
			int sliceNo = 1;
			String sliceStartTime = "";
			String sliceEndTime = "";
			
			String line = in.readLine(); //跳过第一行表头
			while ((line = in.readLine()) != null) {
				String[] values = line.split(",");
				String equip = values[0];
				String dateTime = values[1];
				
				if (!equip.equals(currentEquip)) {
					currentEquip = equip;
					sliceNo = 1;
					sliceStartTime = dateTime;
					sliceEndTime = dateTime;
				} else {
					long lastDateTime = DateTimeTransverter.getMilliseconds(sliceEndTime);
					long currDateTime = DateTimeTransverter.getMilliseconds(dateTime);
					if (currDateTime - lastDateTime > SLICE_SPACING_MILLISECOND) {
						out.write(equip + "," + sliceNo + "," + sliceStartTime + "," + sliceEndTime + "\r\n");
						
						sliceNo++;
						sliceStartTime = dateTime;
						sliceEndTime = dateTime;
					} else {
						sliceEndTime = dateTime;
					}
				}
			}
			in.close();
			out.close();
			System.out.println("[INFO] Sliced " + inputParaFilePath 
					+ " and written slices to " + outputSliceFilePath + "!"); //INFO
			
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
	 * 从默认工况文件夹读取工况文件数据，逐个切分后，把切片信息保存到默认切片文件夹。
	 */
	public void sliceParas() {
		File outputFolder = new File(SLICES_FOLDER_PATH);
		if (!outputFolder.exists() || !outputFolder.isDirectory()) {
			outputFolder.mkdir();
		}

		File inputFolder = new File(PARAS_FOLDER_PATH);
		if (inputFolder.exists() && inputFolder.isDirectory()) {
			File[] files = inputFolder.listFiles();
			for (File file : files) {
				slicePara(file.getPath(), SLICES_FOLDER_PATH + "slice_" + file.getName());
			}
		}
		
		System.out.println("[INFO] Sliced all para files in folder " + PARAS_FOLDER_PATH 
				+ "and saved slices into folder " + SLICES_FOLDER_PATH + "!"); //INFO
	}
	
	
	/**
	 * 读取默认切片文件夹中的单工况切片信息，把它们合并到一个切片文件，并保存到指定文件中。
	 * @param outputFilePath
	 */
	public void mergeSlices(String outputFilePath) {
		try {
			File inputSlicesFolder = new File(SLICES_FOLDER_PATH);
			if (!inputSlicesFolder.exists() || !inputSlicesFolder.isDirectory()) return;
			
			// 将每台设备的所有切片放到一个PriorityQueue中，然后与设备编号用Map对应起
			Map<String, PriorityQueue<Slice>> equipSlicesMap = new HashMap<String, PriorityQueue<Slice>>();
			
			BufferedReader in = null;
			File[] sliceFiles = inputSlicesFolder.listFiles();
			for (File file : sliceFiles) {
				in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "gbk"));
				String line = in.readLine();
				while ((line = in.readLine()) != null) {
					String[] values = line.split(",");
					String equip = values[0];
					String sliceStartTime = values[2];
					String sliceEndTime = values[3];
					
					long startTime = DateTimeTransverter.getMilliseconds(sliceStartTime);
					long endTime = DateTimeTransverter.getMilliseconds(sliceEndTime);
					Slice slice = new Slice(startTime, endTime);
					
					if (!equipSlicesMap.containsKey(equip)) {
						@SuppressWarnings("unchecked")
						PriorityQueue<Slice> sliceQueue = new PriorityQueue<Slice>(20, new SliceComparator()); //一个月的切片数量一般不超过20
						sliceQueue.offer(slice);
						equipSlicesMap.put(equip, sliceQueue);
					} else {
						equipSlicesMap.get(equip).offer(slice);
					}
				}
				in.close();
			}
			System.out.println("[INFO] Read all paras' slice in folder " + SLICES_FOLDER_PATH + " into memory !"); //INFO
			
			// 合并不同工况的切片，并输出到文件
			OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(new File(outputFilePath)), "gbk");
			out.write("EquipID" + "," + "SliceNo" + "," + "StartTime" + "," + "EndTime" + "\r\n");
			for (String equip : equipSlicesMap.keySet()) {
				PriorityQueue<Slice> sliceQueue = equipSlicesMap.get(equip);
				Slice currSlice = null;
				int sliceNo = 1;
				while (!sliceQueue.isEmpty()) {
//					System.out.println("[TEST] " + DateTimeTransverter.parseDateTime(sliceQueue.peek().startTime) 
//							+ " " + DateTimeTransverter.parseDateTime(sliceQueue.peek().endTime)); //TEST
					if (currSlice == null) {
						currSlice = sliceQueue.peek();
					} else {
						if (sliceQueue.peek().startTime > currSlice.endTime) {
//							System.out.println("[TEST] write " + DateTimeTransverter.parseDateTime(currSlice.startTime) 
//									+ " " + DateTimeTransverter.parseDateTime(currSlice.endTime)); //TEST
							out.write(equip + "," + sliceNo + "," 
									+ DateTimeTransverter.parseDateTime(currSlice.startTime) + "," 
									+ DateTimeTransverter.parseDateTime(currSlice.endTime) + "\r\n");
							currSlice = sliceQueue.peek();
							sliceNo++;
						} else {
							currSlice.endTime = Math.max(currSlice.endTime, sliceQueue.peek().endTime);
						}
					}
					sliceQueue.poll();
				}
				out.write(equip + "," + sliceNo + "," 
						+ DateTimeTransverter.parseDateTime(currSlice.startTime) + "," 
						+ DateTimeTransverter.parseDateTime(currSlice.endTime) + "\r\n");
			}
			out.close();
			System.out.println("[INFO] Merged all paras' slice in folder " + SLICES_FOLDER_PATH 
					+ " and written into file " + outputFilePath + "!"); //INFO
			
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
	 * 读取默认切片文件夹中的单工况切片信息，把它们合并到一个切片文件，并保存到默认文件中。
	 */
	public void mergeSlices() {
		mergeSlices(FINAL_SLICE_FEIL_PATH);
	}
	
	
	/**
	 * 比较两个切片的时间先后
	 * @author liangjb
	 */
	@SuppressWarnings("rawtypes")
	public class SliceComparator implements Comparator {
		@Override
		public int compare(Object o1, Object o2) {
			Slice s1 = (Slice) o1;
			Slice s2 = (Slice) o2;
			if (s1.startTime < s2.startTime) return -1;
			else if (s1.startTime > s2.startTime) return 1;
			else {
				if (s1.endTime < s2.endTime) return -1;
				else if (s1.endTime > s2.endTime) return 1;
				else return 0;
			}
		}
	}
	
}
