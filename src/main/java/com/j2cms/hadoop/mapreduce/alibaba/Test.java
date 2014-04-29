package com.j2cms.hadoop.mapreduce.alibaba;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String sd [][] =  {
				{"3","4","5","6","7"},
				{"8.10","8.01","7.15","6.25","6.01"},
		};
//		
//
//		String sd [][] =  {
//				{"3","8.10"},
//				{"4","8.01"},
//				{"5","7.15"},
//				{"6","6.25"},
//				{"7","6.01"}
//		};
		System.out.println(StringUtils.join(sd[0], "_"));
		
//		   Date d=new Date();   
//		   SimpleDateFormat df=new SimpleDateFormat("MM-dd");
//		   System.out.println("今天的日期："+df.format(d));   
//		   System.out.println("两天前的日期：" + df.format(new Date(d.getTime() - 4 * 24 * 60 * 60 * 1000)));  
//		   System.out.println("三天后的日期：" + df.format(new Date(d.getTime() + 3 * 24 * 60 * 60 * 1000)));
//		   
//		   GregorianCalendar gc=new GregorianCalendar(); 
//		   gc.setTime(new Date()); 
//		   gc.add(5,-3); 
//		   
//		   String s = "10944750,15761,0,424";
//		   s=s.substring(0, s.length()-2)+"."+s.substring(s.length()-2);
//		   System.out.println(s);
		
		
		List<String> inputs = new ArrayList<String>(); 
		inputs.add("3_8.01");
		inputs.add("5_8.10");
		System.out.println(inputs.toString());
		System.out.println(StringUtils.join(inputs,"\n"));
		
		List<String[]> sdsList = new ArrayList<String[]>();
		
		sdsList.add(new String[]{"1","2"});
		sdsList.add(new String[]{"3","4"});
		
//		String[][] sdss =  new String[sds.size()][];
		
		String[][] sdss = (String [][]) sdsList.toArray(new String[0][0]);
		
		for(String [] sds:sdss ){
			for(String s:sds){
				System.out.println(s);
			}
		}
		
		
		System.out.println(new String[]{"10","12"});

	}

}
