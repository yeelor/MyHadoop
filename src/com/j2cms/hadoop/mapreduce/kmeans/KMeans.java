package com.j2cms.hadoop.mapreduce.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans {

	public static void main(String[] args) throws Exception {
		// args = new String[]{"hdfs://lenovo0:9000/user/hadoop/kmeans/input",
		// "hdfs://lenovo0:9000/user/hadoop/kmeans/center",
		// "hdfs://lenovo0:9000/user/hadoop/kmeans/output"};

		args = new String[] { "kmeans/input", "kmeans/center", "kmeans/output" };
		CenterInitial centerInitial = new CenterInitial();
		centerInitial.run(args);// 初始化中心点
		int times = 0;
		double s = 0, threshold = 0.1;// threshold是阀值
		do {
			Configuration conf = new Configuration();
			conf.set("mapred.job.tracker", "lenovo0:9001");

			// conf.set("fs.default.name", "hdfs://lenovo0:9000");
			Job job = new Job(conf, "KMeans_Iterator" + times);// 建立KMeans的MapReduce作业
			job.setJarByClass(KMeans.class);// 设定作业的启动类
			job.setOutputKeyClass(Text.class);// 设定Key输出的格式：Text
			job.setOutputValueClass(Text.class);// 设定value输出的格式：Text
			job.setMapperClass(KMapper.class);// 设定Mapper类
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);// 设定Reducer类
			job.setReducerClass(KReducer.class);
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(args[2]), true);// args[2]是output目录，fs.delete是将已存在的output删除
			// 解析输入和输出参数，分别作为作业的输入和输出，都是文件
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			// 运行作业并判断是否完成成功
			job.waitForCompletion(true);
			if (job.waitForCompletion(true))// 上一次mapreduce过程结束
			{
				// 上两个中心点做比较，如果中心点之间的距离小于阈值就停止；如果距离大于阈值，就把最近的中心点作为新中心点
				NewCenter newCenter = new NewCenter();
				s = newCenter.run(args);
				times++;
			}
		} while (s > threshold);// 当误差小于阈值停止。
		System.out.println("Iterator: " + times);// 迭代次数
	}

}
