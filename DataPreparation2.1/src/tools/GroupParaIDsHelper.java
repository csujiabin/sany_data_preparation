package tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GroupParaIDsHelper {
	
	public void groupParaIDs(String inputParaIDsFile, int groups) {
		try {
			// Read paraIDs from file
			
			List<String> paraIDList = new ArrayList<String>();
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(inputParaIDsFile), "gbk"));
			String line = "";
			while ((line = in.readLine()) != null) {
				if (!line.trim().isEmpty()) {
					paraIDList.add(line);
				}
			}
			in.close();
			
			// Shuffle the paraIDs
			
			String[] paras = paraIDList.toArray(new String[0]);
			Random random = new Random();
			for (int i = paras.length - 1; i > 0; i--) {
				int index = random.nextInt(i);
				String tmp = paras[i];
				paras[i] = paras[index];
				paras[index] = tmp;
			}
			
			OutputStreamWriter out = null;
			for (int i = 0; i < groups; i++) {
				String fileName = "paras_" + (i + 1) + ".csv";
				out = new OutputStreamWriter(new FileOutputStream(new File(fileName)), "gbk");
				
				for (int j = 0 + i; j < paras.length; j += groups) {
					out.write(paras[j] + "\r\n");
				}
				
				out.close();
			}
			
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	public static void main(String[] args) {
		new GroupParaIDsHelper().groupParaIDs("all_paras.csv", 8);
	}

}
