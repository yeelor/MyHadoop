package com.j2cms.hadoop.mapreduce.alibaba;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

public class Aconst {

	public static final String rootPath = "/user/hadoop/alibaba/";

	public static final String date = new SimpleDateFormat("yyyyMMdd").format(new Date());

	// e.g. /user/hadoop/alibaba/20140402/
	public static final String rootPathPlusDate = rootPath + date + "/";

	// 点击：0；购买：1；收藏：2；购物车：3 { 1, 1000, 10, 100 };
	public static final Integer weights[] = { 1, 10, 5, 8 }; // 2014-04-03

	public static final float firstDay =(float)8.15;
	
	public static final float lastDay = (float)4.15;
	
	// e.g. 1_10_5_8
	public static final String ws = firstDay+"_"+StringUtils.join(weights, "_");

	// e.g. /user/hadoop/alibaba/1_10_5_8
	public static final String rootPathPlusWs = rootPath + ws + "/";
	
	// e.g. /user/hadoop/alibaba/20140402/1_10_5_8
	public static final String rootPathPlusDatePlusWs = rootPath + date + "_"+ ws + "/";

	public static final String master = "lenovo0:9001";
	
	public static final String aliFile = rootPath+"t_data.txt";
	
	//最后一个月的购买记录数
	public static final int bBrand =1378;// 1408;
	
}
