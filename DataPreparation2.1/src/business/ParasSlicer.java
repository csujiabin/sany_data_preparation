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

import org.apache.log4j.Logger;

import model.Slice;
import util.DateTimeTransverter;

public class ParasSlicer {
	
	private static Logger logger = Logger.getLogger(business.ParasSlicer.class);
	
	/**
	 * 划分切片依据的7.4个小时转换成的毫秒数。
	 */
	private static final long SLICE_SPACING_MILLISECOND = (long) (7.4 * 3600 * 1000);
	
	/**
	 * 读入设备号，查询指定时间段的6个工况数据，分别计算切片，然后合并到的一起得到最终的切片数据，保存到文件。
	 * @param equipsFilePath
	 * @param startDateTime
	 * @param endDateTime
	 * @param slicesFilePath
	 */
	public void generateSlices(String equipsFilePath, String startDateTime, String endDateTime, String slicesFilePath) {
		
		// 导出6个工况数据。
		
		String[] parasID = {"480", "481", "305", "869", "276", "277"};
		String parasFolderPath = "paras/";
		ParasDumper parasDumper = new ParasDumper();
		parasDumper.dumpParas(parasID, equipsFilePath, startDateTime, endDateTime, parasFolderPath);
		
		// 对每个工况划分切片。
		
		String slicesFolderPath = "paras_slices/";
		ParasSlicer parasSlicer = new ParasSlicer();
		parasSlicer.sliceParas(parasFolderPath, slicesFolderPath);
		
		// 把6个工况分别得到的切片合并。
		
		parasSlicer.mergeFlagParasSlices(slicesFolderPath, slicesFilePath);
		
		// 删除中间文件，即每个工况划分切片的结果。
		
		File slicesFolder = new File(slicesFolderPath);
		if (slicesFolder.exists()) {
			if (slicesFolder.isDirectory()) {
	            File[] children = slicesFolder.listFiles();
	            for (File child : children) {
	            	child.delete();
	            }
	        }
			slicesFolder.delete();
			logger.info("delete all files in folder: " + slicesFolderPath);
		}
	}
	
	
	/**
	 * 从输入文件夹中读取6个工况文件，分别划分切片并保存到输出文件夹。
	 * @param inputFolderPath
	 * @param outputFolderPath
	 */
	public void sliceParas(String inputFolderPath, String outputFolderPath) {
		
		// 创建输出文件夹。
		
		File outputFolder = new File(outputFolderPath);
		if (!outputFolder.exists() || !outputFolder.isDirectory()) {
			outputFolder.mkdir();
			logger.info("make folder: " + outputFolderPath);
		}
		
		// 检查输入文件夹是否存在。

		File inputFolder = new File(inputFolderPath);
		if (!inputFolder.exists() || !inputFolder.isDirectory()) {
			logger.error("folder doesn't exist: " + inputFolderPath);
			return;
		}
		
		// 对输入文件夹中每个工况文件划分切片。
		
		File[] files = inputFolder.listFiles();
		for (File file : files) {
			slicePara(file.getPath(), outputFolderPath + "slice_" + file.getName());
		}
		
		logger.info("generated slices for all paras in folder: " + inputFolderPath);
	}
	
	
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
			int sliceNo = 0;
			long sliceStartTime = -1;
			long sliceEndTime = -1;
			
			String line = in.readLine(); //跳过第一行表头
			while ((line = in.readLine()) != null) {
				String[] columns = line.split(",");
				
				if (columns.length < 2) {
					logger.error("columns < 2");
					continue;
				}
				
				String equip = columns[0];
				long dateTime = DateTimeTransverter.getMilliseconds(columns[1]);
				
				if (!equip.equals(currentEquip)) {
					if (sliceStartTime != -1 && sliceEndTime != -1) {
						out.write(currentEquip + "," + sliceNo + "," 
								+ DateTimeTransverter.parseDateTime(sliceStartTime) + "," 
								+ DateTimeTransverter.parseDateTime(sliceEndTime) + "\r\n");
					}
					
					currentEquip = equip;
					sliceNo = 0;
					sliceStartTime = dateTime;
					sliceEndTime = dateTime;
				} else {
					if (dateTime - sliceEndTime > SLICE_SPACING_MILLISECOND) {
						out.write(equip + "," + sliceNo + "," 
								+ DateTimeTransverter.parseDateTime(sliceStartTime) + "," 
								+ DateTimeTransverter.parseDateTime(sliceEndTime) + "\r\n");
						
						sliceNo++;
						sliceStartTime = dateTime;
						sliceEndTime = dateTime;
					} else {
						sliceEndTime = Math.max(sliceEndTime, dateTime);
					}
				}
			}
			out.write(currentEquip + "," + sliceNo + "," 
					+ DateTimeTransverter.parseDateTime(sliceStartTime) + "," 
					+ DateTimeTransverter.parseDateTime(sliceEndTime) + "\r\n");
			
			in.close();
			out.close();

			logger.info("generated slice for input file: " + inputParaFilePath);
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 读取文件夹中的6个工况划分的切片，把它们合并成一个切片文件。
	 * @param inputFolderPath
	 * @param outputFilePath
	 */
	public void mergeFlagParasSlices(String inputFolderPath, String outputFilePath) {
		try {
			
			// 检查输入文件夹是否存在。
			
			File inputSlicesFolder = new File(inputFolderPath);
			if (!inputSlicesFolder.exists() || !inputSlicesFolder.isDirectory()) {
				logger.error("folder doesn't exist: " + inputFolderPath);
				return;
			}
			
			// 将每台设备的所有切片放到一个优先队列中，然后与设备编号用Map对应起。
			
			Map<String, PriorityQueue<Slice>> equipSlicesMap = new HashMap<String, PriorityQueue<Slice>>();
			
			BufferedReader in = null;
			File[] sliceFiles = inputSlicesFolder.listFiles();
			for (File file : sliceFiles) {
				in = new BufferedReader(new InputStreamReader(new FileInputStream(file), "gbk"));
				String line = in.readLine();
				while ((line = in.readLine()) != null) {
					String[] values = line.split(",");
					
					if (values.length < 4) {
						logger.error("columns < 4");
						continue;
					}
					
					String equip = values[0];
					String sliceStartTime = values[2];
					String sliceEndTime = values[3];
					
					long startTime = DateTimeTransverter.getMilliseconds(sliceStartTime);
					long endTime = DateTimeTransverter.getMilliseconds(sliceEndTime);
					Slice slice = new Slice(startTime, endTime);
					
					if (!equipSlicesMap.containsKey(equip)) {
						PriorityQueue<Slice> sliceQueue = new PriorityQueue<Slice>(20, new SliceComparator()); //一个月的切片数量一般不超过20
						sliceQueue.offer(slice);
						equipSlicesMap.put(equip, sliceQueue);
					} else {
						equipSlicesMap.get(equip).offer(slice);
					}
				}
				in.close();
			}
			
			logger.info("read all slices in folder: " + inputFolderPath);
			
			
			// 合并不同工况的切片，并输出到文件。
			
			OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(new File(outputFilePath)), "gbk");
			out.write("EquipID" + "," + "SliceNo" + "," + "StartTime" + "," + "EndTime" + "\r\n");
			
			for (String equip : equipSlicesMap.keySet()) {
				PriorityQueue<Slice> sliceQueue = equipSlicesMap.get(equip);
				Slice currSlice = null;
				int sliceNo = 0;
				while (!sliceQueue.isEmpty()) {
					if (currSlice == null) {
						currSlice = sliceQueue.peek();
						continue;
					}
					
					if (sliceQueue.peek().startTime - currSlice.endTime > SLICE_SPACING_MILLISECOND) {
						out.write(equip + "," + sliceNo + "," 
								+ DateTimeTransverter.parseDateTime(currSlice.startTime) + "," 
								+ DateTimeTransverter.parseDateTime(currSlice.endTime) + "\r\n");
						currSlice = sliceQueue.peek();
						sliceNo++;
					} else {
						currSlice.endTime = Math.max(currSlice.endTime, sliceQueue.peek().endTime);
					}
					
					sliceQueue.poll();
				}
				out.write(equip + "," + sliceNo + "," 
						+ DateTimeTransverter.parseDateTime(currSlice.startTime) + "," 
						+ DateTimeTransverter.parseDateTime(currSlice.endTime) + "\r\n");
			}
			out.close();
			
			logger.info("merged all slices in folder: " + inputFolderPath);
			logger.info("writtern merged slices into file: " + outputFilePath);
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 比较两个切片的时间先后
	 * @author jiabin
	 */
	public class SliceComparator implements Comparator<Object> {
		@Override
		public int compare(Object o1, Object o2) {
			Slice s1 = (Slice) o1;
			Slice s2 = (Slice) o2;
			if (s1.startTime < s2.startTime) return -1;
			if (s1.startTime > s2.startTime) return 1;
			return 0;
		}
	}
	
}
