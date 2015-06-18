package util;

import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateTimeTransverter {
	
	/**
	 * ��ʱ���ת��Ϊ����ʱ��
	 * @param milliseconds ��1970-01-01 08:00:00�����ĺ�����
	 * @return "yyyy-MM-dd HH:mm:ss"��ʽ������ʱ��
	 */
	public static String parseDateTime(long milliseconds) {
		Date date = new Date(milliseconds);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(date);
	}
	
	/**
	 * ������ʱ��ת��Ϊʱ���
	 * @param dateTime "yyyy-MM-dd HH:mm:ss"��ʽ������ʱ��
	 * @return ��1970-01-01 08:00:00�����ĺ�����
	 */
	public static long getMilliseconds(String dateTime) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Date date = sdf.parse(dateTime);
			return date.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
			return 0;
		}
	}
	
	
	/**
	 * ����"yyyy-MM-dd HH:mm:ss"��ʽ�ĵ�ǰ����ʱ��
	 * @return
	 */
	public static String getCurrentDateTime() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date());
	}
	
	
	/**
	 * ���ص�ǰʱ��ĺ�����
	 * @return
	 */
	public static long getCurrentMilliseconds() {
		return new Date().getTime();
	}

	
	public static void main(String[] args) {
		// FOR TEST
		System.out.println(DateTimeTransverter.getMilliseconds("2012-12-01 00:00:00"));
		System.out.println(DateTimeTransverter.getMilliseconds("2012-12-31 23:59:59"));
		System.out.println(DateTimeTransverter.parseDateTime(Long.parseLong("0")));
		System.out.println(DateTimeTransverter.parseDateTime(Long.parseLong("1349334099000")));
	}

}
