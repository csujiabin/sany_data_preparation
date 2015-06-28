package util;

import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DateTimeTransverter {
	
	/**
	 * 把时间戳转换为日期时间
	 * @param milliseconds 自1970-01-01 08:00:00以来的毫秒数
	 * @return "yyyy-MM-dd HH:mm:ss"格式的日期时间
	 */
	public static String parseDateTime(long milliseconds) {
		Date date = new Date(milliseconds);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(date);
	}
	
	/**
	 * 把日期时间转换为时间戳
	 * @param dateTime "yyyy-MM-dd HH:mm:ss"格式的日期时间
	 * @return 自1970-01-01 08:00:00以来的毫秒数
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
	 * 返回"yyyy-MM-dd HH:mm:ss"格式的当前日期时间
	 * @return
	 */
	public static String getCurrentDateTime() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(new Date());
	}
	
	
	/**
	 * 返回当前时间的毫秒数
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
