package com.j2cms.hadoop.mapreduce.kmeans;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMapper extends Mapper<LongWritable, Text, Text, Text> {

	private String[] center;

	// 读取更新的中心点坐标，并将坐标存入center数组中
	protected void setup(Context context) throws IOException, InterruptedException {
		String centerlist = "kmeans/center/2.txt"; //聚类中心文件
		Configuration conf = new Configuration();
		// conf1.set("hadoop.job.ugi", "hadoop-user,hadoop-user");
		FileSystem fs = FileSystem.get(URI.create(centerlist), conf);
		FSDataInputStream in = null;
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			in = fs.open(new Path(centerlist));
			IOUtils.copyBytes(in, out, 100, false);
			center = out.toString().split(" ");
		} finally {
			IOUtils.closeStream(in);
		}
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		// 从input读入数据，以空格为分割符，一个一个处理
		while (itr.hasMoreTokens())// 用于判断所要分析的字符串中，是否还有语言符号，如果有则返回true，反之返回false
		{
			// 计算第一个坐标跟第一个中心的距离min
			String outValue = new String(itr.nextToken());// 逐个获取以空格为分割符的字符串(2,3)(10,30)(34,40)(1,1)
			String[] list = outValue.replace("(", "").replace(")", "").split(",");
			float min = Float.MAX_VALUE;
			int pos = Integer.MAX_VALUE;

			for (int i = 0; i < center.length; i++) {
				String[] centerStrings = center[i].replace("(", "").replace(")", "").split(",");
				float distance = 0;
				for (int j = 0; j < list.length; j++)
					distance += (float) Math.pow((Float.parseFloat(list[j]) - Float.parseFloat(centerStrings[j])), 2);
				System.out.println("当前计算点" + outValue + "距离第" + i + "个中心点" + center[i] + "的距离为" + distance);
				if (min > distance) {
					min = distance;
					pos = i;
				}
			}
			context.write(new Text(center[pos]), new Text(outValue));// 输出：中心点，对应的坐标
			System.out.println("选中第" + pos + "个中点" + center[pos] + "作为当前计算点所划分的聚类中心");
			System.out.println("Mapper输出 <中心点坐标,计算点坐标>：<" + center[pos] + "," + outValue + ">");
		}
	}

}