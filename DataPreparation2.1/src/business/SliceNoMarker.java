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

import org.apache.log4j.Logger;

import util.DateTimeTransverter;
import model.Slice;

public class SliceNoMarker {
	
	private static Logger logger = Logger.getLogger(business.SliceNoMarker.class);

	/**
	 * 根据指定的切片文件，对输入文件夹中的每一个工况文件，进行切片号标记。
	 * @param inputParasFolderPath
	 * @param inputSliceFilePath
	 * @param outputMarkedParasFolderPath
	 */
	public void markSliceNo(String inputParasFolderPath, String inputSliceFilePath, String outputMarkedParasFolderPath) {
		
		// 把所有设备的切片数据载入内存，每台设备对应的切片用一个TreeMap存储。
		HashMap<String, TreeMap<Integer, Slice>> equipSliceMap = new HashMap<String, TreeMap<Integer, Slice>>();
		
		try {
			// 检查输入切片文件是否存在。
			File sliceFile = new File(inputSliceFilePath);
			if (!sliceFile.exists()) {
				logger.error("file doesn't exit: " + sliceFile);
				return;
			}
			
			// 检查输入工况文件夹是否存在。
			File parasFolder = new File(inputParasFolderPath);
			if (!parasFolder.exists() || !parasFolder.isDirectory()) {
				logger.error("folder doesn't exit: " + parasFolder);
				return;
			}
			
			// 创建输出文件夹。
			File markedParaFolder = new File(outputMarkedParasFolderPath);
			if (!markedParaFolder.exists() || !markedParaFolder.isDirectory()) {
				logger.info("make dir: " + markedParaFolder);
				markedParaFolder.mkdir();
			}
			
			// 把每个设备的切片放到一个TreeMap中，然后把所有设备及其对应的切片TreeMap放到一个HashMap中。
			TreeMap<Integer, Slice> sliceTreeMap = null;
			
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(inputSliceFilePath), "gbk"));
			
			String line = in.readLine(); //表头
			while ((line = in.readLine()) != null) {
				String[] columns = line.split(",");
				if (columns.length < 4) {
					logger.error("columns < 4");
					continue;
				}
				
				String equip = columns[0];
				Integer sliceNo = Integer.parseInt(columns[1]);
				String startTime = columns[2];
				String endTime = columns[3];
				
				Slice slice = new Slice(DateTimeTransverter.getMilliseconds(startTime), DateTimeTransverter.getMilliseconds(endTime));
				
				if (equipSliceMap.containsKey(equip)) {
					sliceTreeMap = equipSliceMap.get(equip);
				} else {
					sliceTreeMap = new TreeMap<Integer, Slice>();
					equipSliceMap.put(equip, sliceTreeMap);
				}
				
				sliceTreeMap.put(sliceNo, slice);
			}
			in.close();
			
			logger.info("read slices from file: " + inputSliceFilePath);
			
			// 对于每个工况文件，首先找到每台车对应的切片TreeMap，然后获取每个工况值的切片号。
			
			OutputStreamWriter out = null;
			File[] paraFiles = parasFolder.listFiles();
			for (File paraFile: paraFiles) {
				String paraID = paraFile.getName().split("\\.")[0];
				
				in = new BufferedReader(new InputStreamReader(new FileInputStream(paraFile), "gbk"));
				String outputFilePath = outputMarkedParasFolderPath + "marked_" + paraFile.getName();
				out = new OutputStreamWriter(new FileOutputStream(new File(outputFilePath)), "gbk");
				out.write("ParaID" + "," + "EquipID" + "," + "SliceNo" + "," + "LocalTime" + "," + "ParaValue" + "\r\n");
				
				in.readLine(); //表头
				while ((line = in.readLine()) != null) {
					String[] columns = line.split(",");
					if (columns.length < 3) {
						logger.error("columns < 3");
						continue;
					}
					
					String equip = columns[0];
					String localTime = columns[1];
					String paraValue = columns[2];
					
					int sliceNo = getSliceNo(equipSliceMap.get(equip), DateTimeTransverter.getMilliseconds(localTime));
					out.write(paraID + "," + equip + "," + sliceNo + "," + localTime + "," + paraValue + "\r\n");
				}
				
				out.close();
				in.close();
				
				// 标记切片号后删除原始工况文件。
				paraFile.delete();
				
				logger.info("marked para and written into file: paraID=" + paraID);
			}
			
			// 删除原始工况文件夹。
			parasFolder.delete();
			
			logger.info("marked all paras and written into folder: " + outputMarkedParasFolderPath);
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 根据时间戳获取工况值得切片号，其中一台车的所有切片存放在一个TreeMap中。
	 * @param sliceMap
	 * @param timeStamp
	 * @return 如果切片集合不存在，返回-1；如果该时间不在任何切片中，返回-2；正常返回该时间所在的切片号。
	 */
	public int getSliceNo(TreeMap<Integer, Slice> sliceMap, long timeStamp) {
		if (sliceMap == null) return -1;
		
		int left = 0, right = sliceMap.size() - 1;
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

}
