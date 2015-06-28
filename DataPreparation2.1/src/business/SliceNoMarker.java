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
	 * ����ָ������Ƭ�ļ����������ļ����е�ÿһ�������ļ���������Ƭ�ű�ǡ�
	 * @param inputParasFolderPath
	 * @param inputSliceFilePath
	 * @param outputMarkedParasFolderPath
	 */
	public void markSliceNo(String inputParasFolderPath, String inputSliceFilePath, String outputMarkedParasFolderPath) {
		
		// �������豸����Ƭ���������ڴ棬ÿ̨�豸��Ӧ����Ƭ��һ��TreeMap�洢��
		HashMap<String, TreeMap<Integer, Slice>> equipSliceMap = new HashMap<String, TreeMap<Integer, Slice>>();
		
		try {
			// ���������Ƭ�ļ��Ƿ���ڡ�
			File sliceFile = new File(inputSliceFilePath);
			if (!sliceFile.exists()) {
				logger.error("file doesn't exit: " + sliceFile);
				return;
			}
			
			// ������빤���ļ����Ƿ���ڡ�
			File parasFolder = new File(inputParasFolderPath);
			if (!parasFolder.exists() || !parasFolder.isDirectory()) {
				logger.error("folder doesn't exit: " + parasFolder);
				return;
			}
			
			// ��������ļ��С�
			File markedParaFolder = new File(outputMarkedParasFolderPath);
			if (!markedParaFolder.exists() || !markedParaFolder.isDirectory()) {
				logger.info("make dir: " + markedParaFolder);
				markedParaFolder.mkdir();
			}
			
			// ��ÿ���豸����Ƭ�ŵ�һ��TreeMap�У�Ȼ��������豸�����Ӧ����ƬTreeMap�ŵ�һ��HashMap�С�
			TreeMap<Integer, Slice> sliceTreeMap = null;
			
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(inputSliceFilePath), "gbk"));
			
			String line = in.readLine(); //��ͷ
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
			
			// ����ÿ�������ļ��������ҵ�ÿ̨����Ӧ����ƬTreeMap��Ȼ���ȡÿ������ֵ����Ƭ�š�
			
			OutputStreamWriter out = null;
			File[] paraFiles = parasFolder.listFiles();
			for (File paraFile: paraFiles) {
				String paraID = paraFile.getName().split("\\.")[0];
				
				in = new BufferedReader(new InputStreamReader(new FileInputStream(paraFile), "gbk"));
				String outputFilePath = outputMarkedParasFolderPath + "marked_" + paraFile.getName();
				out = new OutputStreamWriter(new FileOutputStream(new File(outputFilePath)), "gbk");
				out.write("ParaID" + "," + "EquipID" + "," + "SliceNo" + "," + "LocalTime" + "," + "ParaValue" + "\r\n");
				
				in.readLine(); //��ͷ
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
				
				// �����Ƭ�ź�ɾ��ԭʼ�����ļ���
				paraFile.delete();
				
				logger.info("marked para and written into file: paraID=" + paraID);
			}
			
			// ɾ��ԭʼ�����ļ��С�
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
	 * ����ʱ�����ȡ����ֵ����Ƭ�ţ�����һ̨����������Ƭ�����һ��TreeMap�С�
	 * @param sliceMap
	 * @param timeStamp
	 * @return �����Ƭ���ϲ����ڣ�����-1�������ʱ�䲻���κ���Ƭ�У�����-2���������ظ�ʱ�����ڵ���Ƭ�š�
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
