package com.j2cms.hadoop.mapreduce.kmeans;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 初始化中心点
 * 
 */
public class CenterInitial {

	public void run(String[] args) throws IOException {
		String[] clist;// 用于保存中心点
		int k = 2;// 中心点选取个数
		String clusterString = "";// 保存各个中心点在同一个字符串string中
		String inpath = args[0] + "/1.txt"; // cluster数据集放在1.txt中
		String outpath = args[1] + "/2.txt"; // center新选取的中心点放进2.txt中保存
		Configuration conf = new Configuration(); // 读取hadoop文件系统的配置
		//conf1.set("hadoop.job.ugi", "hadoop,hadoop"); // 配置信息设置
		FileSystem fs = FileSystem.get(URI.create(inpath), conf); // FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统
		FSDataInputStream in = null;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {

			in = fs.open(new Path(inpath));
			IOUtils.copyBytes(in, out, 4096, false); // 用Hadoop的IOUtils工具方法来让这个文件的指定字节复制到标准输出流上
			// 把in读到的数据 复制到out上
			clist = out.toString().split(" ");// 将out以空格为分割符转换成数组在clist中保存
		} finally {
			IOUtils.closeStream(in);
		}

		FileSystem filesystem = FileSystem.get(URI.create(outpath), conf); // 获得URI对应的HDFS文件系统

		List<Integer>  randomList = new ArrayList<Integer>();
		
	
		int i =0;
		while(i<k){
			Integer random = (int) (Math.random() * clist.length);
			if(!randomList.contains(random)){
				randomList.add(random);
				i++;
				clusterString = clusterString + clist[random]+ " ";// 将得到的k个随机点的坐标用一个字符串保存
			}
		}
	
		OutputStream out2 = filesystem.create(new Path(outpath));
		IOUtils.copyBytes(new ByteArrayInputStream(clusterString.getBytes()), out2,4096, true); // 把随机点坐标字符串out2中
		System.out.println("初始化的聚类中心为：" + clusterString);
	}

}
