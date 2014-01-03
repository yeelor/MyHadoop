package com.j2cms.hadoop.mapreduce.kmeans;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class NewCenter {
	
	int k = 2;
	float shold=Integer.MIN_VALUE;
	String[] line;
	String newcenter = new String("");
	
	public float run(String[] args) throws IOException,InterruptedException
	{
		Configuration conf = new Configuration();
//		conf.set("hadoop.job.ugi", "hadoop,hadoop"); 
		FileSystem fs = FileSystem.get(URI.create(args[2]+"/part-r-00000"),conf);
		FSDataInputStream in = null;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try{ 
			in = fs.open( new Path(args[2]+"/part-r-00000")); 
			IOUtils.copyBytes(in,out,50,false);
			line = out.toString().split("\n");
			} finally { 
				IOUtils.closeStream(in);
			}
	
		//System.out.println("上一次的MapReduce结果："+out.toString());
		System.out.println("上一次MapReduce结果：\n第一行："+line[0]);
		System.out.println("第二行："+line[1]);
		for(int i=0;i<k;i++)
		{
			String[] l = line[i].replace("\t", " ").split(" ");//如果这行有tab的空格，可以替代为空格
			//(key,values)key和values同时输出是，中间保留一个Tab的距离，即'\t'
			String[] startCenter = l[0].replace("(", "").replace(")", "").split(",");
			//上上次的中心点startCenter[0]=(10,30);startCenter[1]=(2,3);
			String[] finalCenter = l[l.length-1].replace("(", "").replace(")", "").split(",");
			//上一次的中心点finalCenter[0]=(22,35);finalCenter[1]=(1.5,2.0);
			float tmp = 0;
			for(int j=0;j<startCenter.length;j++)
				tmp += Math.pow(Float.parseFloat(startCenter[j])-Float.parseFloat(finalCenter[j]), 2);
			//两个中心点间的欧式距离的平方
			newcenter = newcenter + l[l.length - 1].replace("\t", "") + " ";
			if(shold <= tmp)
				shold = tmp;
			System.out.println("新旧中心点的"+i+"坐标距离："+tmp);
		}
		OutputStream out2 = fs.create(new Path(args[1]+"/2.txt") ); 
		IOUtils.copyBytes(new ByteArrayInputStream(newcenter.getBytes()), out2, 4096,true);
		//System.out.println(newcenter);
		return shold;
		//return 0;
	}

}

