package com.j2cms.hadoop.mapreduce.alibaba;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class T1 {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		private static String line = new String();// 每行数据

		static int score = 0;
		static float startDay = (float) 4.1;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			startDay = Float.valueOf(context.getConfiguration().get("startDay"));
		}

		// 实现map函数
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			line = value.toString();
			// String [] s = string.split(" ");//这是以空格拆分字符串
			// String [] s = string.split("\\s+");//1个或多个空格
			// String [] s = string.split("\\s|[\\s]|[^\\S]");//一个空格|竖线是或者的意思。

			if ((line != null) && (!line.equals(""))) {

				String values[] = line.split(",");// 1个或多个空格

				String userId = values[0];
				String brand = values[1];
				String type = values[2];
				float visitDay = Float.valueOf(values[3]);

				// System.out.println("day="+day);

				if ((visitDay >= startDay)&&(visitDay <= Aconst.firstDay)) {
//				if ((visitDay >= day)&&(visitDay<6.01)) {
					context.write(new Text(userId + "_" + brand), new Text(type));
				}
			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// 实现reduce函数
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int score = 0;
			for (Text v : values) {
				score += Aconst.weights[Integer.valueOf(v.toString())];
			}
			context.write(key, new Text(String.valueOf(score)));
		}
	}

	public static void job(String input, String output, String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", Aconst.master);

		// 设置两个参数
		conf.set("startDay", args[1]);

		Job job = new Job(conf, "T1_" + args[0] + "_" + args[1]);
		job.setJarByClass(T1.class);
		job.setMapperClass(Map.class);
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 默认,可省略不写
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);

	}

	/**
	 * 
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws Exception {
		String date = new SimpleDateFormat("yyyyMMdd").format(new Date());
		String directory = "/user/hadoop/alibaba/";
		// 输入输出目录
		String input = directory + "t_data.txt";
		String ouput = directory + date + "/" + args[0] + "_" + args[1];

		T1.job(input,ouput,args);
		HDFSUtil.deleteNoUse(ouput);

		// 输入输出目录
		input = ouput;
		ouput = directory + date + "/" + args[0] + "_" + args[1] + "_result";

		T3.job(input,ouput,args);
		HDFSUtil.deleteNoUse(ouput);
	}

}
