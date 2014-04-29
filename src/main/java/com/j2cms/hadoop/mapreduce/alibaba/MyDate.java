package com.j2cms.hadoop.mapreduce.alibaba;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;

public class MyDate {

	/**
	*java中对日期的加减操作
	*gc.add(1,-1)表示年份减一.
	*gc.add(2,-1)表示月份减一.
	*gc.add(3.-1)表示周减一.
	*gc.add(5,-1)表示天减一.
	*以此类推应该可以精确的毫秒吧.没有再试.大家可以试试.
	*GregorianCalendar类的add(int field,int amount)方法表示年月日加减.
	*field 参数表示年,月.日等.
	*n 参数表示要加减的数量.
	*/
	public static String getDay(String day,int field,int n){
		GregorianCalendar gc = new GregorianCalendar();
		try {
			gc.setTime(new SimpleDateFormat("yyyyMMdd").parse(day));
			gc.add(field, n);
		} catch (ParseException e) {

			e.printStackTrace();
		}
//		day = new SimpleDateFormat("yyyyMMdd").format(gc.getTime());
		day = new SimpleDateFormat("MM.dd").format(gc.getTime());
		return day;
	}
	
	/**
	 * @param day 当前天
	 * @param n  减少的天数
	 * @return
	 */
	public static String getDaysBefore(String day,int n){
		if(day.length()==4)
			day="0"+day;
		String d = ("2014"+day);
		d = d.replace(".", "");
//		System.out.println(d);
		
		d = getDay(d,5,-Integer.valueOf(n));

		if(d.startsWith("0"))
			d=d.substring(1);
		
		return d;
	}
	
	public static void main(String[] args) {
		System.out.println(MyDate.getDay("20140401", 5, -3));
		System.out.println(MyDate.getDaysBefore("4.13", 3));
		
		
	}
}
