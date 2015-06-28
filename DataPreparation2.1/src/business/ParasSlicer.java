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
	 * ������Ƭ���ݵ�7.4��Сʱת���ɵĺ�������
	 */
	private static final long SLICE_SPACING_MILLISECOND = (long) (7.4 * 3600 * 1000);
	
	/**
	 * �����豸�ţ���ѯָ��ʱ��ε�6���������ݣ��ֱ������Ƭ��Ȼ��ϲ�����һ��õ����յ���Ƭ���ݣ����浽�ļ���
	 * @param equipsFilePath
	 * @param startDateTime
	 * @param endDateTime
	 * @param slicesFilePath
	 */
	public void generateSlices(String equipsFilePath, String startDateTime, String endDateTime, String slicesFilePath) {
		
		// ����6���������ݡ�
		
		String[] parasID = {"480", "481", "305", "869", "276", "277"};
		String parasFolderPath = "paras/";
		ParasDumper parasDumper = new ParasDumper();
		parasDumper.dumpParas(parasID, equipsFilePath, startDateTime, endDateTime, parasFolderPath);
		
		// ��ÿ������������Ƭ��
		
		String slicesFolderPath = "paras_slices/";
		ParasSlicer parasSlicer = new ParasSlicer();
		parasSlicer.sliceParas(parasFolderPath, slicesFolderPath);
		
		// ��6�������ֱ�õ�����Ƭ�ϲ���
		
		parasSlicer.mergeFlagParasSlices(slicesFolderPath, slicesFilePath);
		
		// ɾ���м��ļ�����ÿ������������Ƭ�Ľ����
		
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
	 * �������ļ����ж�ȡ6�������ļ����ֱ𻮷���Ƭ�����浽����ļ��С�
	 * @param inputFolderPath
	 * @param outputFolderPath
	 */
	public void sliceParas(String inputFolderPath, String outputFolderPath) {
		
		// ��������ļ��С�
		
		File outputFolder = new File(outputFolderPath);
		if (!outputFolder.exists() || !outputFolder.isDirectory()) {
			outputFolder.mkdir();
			logger.info("make folder: " + outputFolderPath);
		}
		
		// ��������ļ����Ƿ���ڡ�

		File inputFolder = new File(inputFolderPath);
		if (!inputFolder.exists() || !inputFolder.isDirectory()) {
			logger.error("folder doesn't exist: " + inputFolderPath);
			return;
		}
		
		// �������ļ�����ÿ�������ļ�������Ƭ��
		
		File[] files = inputFolder.listFiles();
		for (File file : files) {
			slicePara(file.getPath(), outputFolderPath + "slice_" + file.getName());
		}
		
		logger.info("generated slices for all paras in folder: " + inputFolderPath);
	}
	
	
	/**
	 * ���ļ��ж�ȡ���������ݣ�������Ƭ�����浽�ļ���
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
			
			String line = in.readLine(); //������һ�б�ͷ
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
	 * ��ȡ�ļ����е�6���������ֵ���Ƭ�������Ǻϲ���һ����Ƭ�ļ���
	 * @param inputFolderPath
	 * @param outputFilePath
	 */
	public void mergeFlagParasSlices(String inputFolderPath, String outputFilePath) {
		try {
			
			// ��������ļ����Ƿ���ڡ�
			
			File inputSlicesFolder = new File(inputFolderPath);
			if (!inputSlicesFolder.exists() || !inputSlicesFolder.isDirectory()) {
				logger.error("folder doesn't exist: " + inputFolderPath);
				return;
			}
			
			// ��ÿ̨�豸��������Ƭ�ŵ�һ�����ȶ����У�Ȼ�����豸�����Map��Ӧ��
			
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
						PriorityQueue<Slice> sliceQueue = new PriorityQueue<Slice>(20, new SliceComparator()); //һ���µ���Ƭ����һ�㲻����20
						sliceQueue.offer(slice);
						equipSlicesMap.put(equip, sliceQueue);
					} else {
						equipSlicesMap.get(equip).offer(slice);
					}
				}
				in.close();
			}
			
			logger.info("read all slices in folder: " + inputFolderPath);
			
			
			// �ϲ���ͬ��������Ƭ����������ļ���
			
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
	 * �Ƚ�������Ƭ��ʱ���Ⱥ�
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
