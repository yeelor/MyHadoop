package com.j2cms.hadoop.mapreduce.alibaba;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 将日期5.4转换成5.04
 * @author aleak
 *
 */
public class InitialData {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		private static String line = new String();// 每行数据

		// 实现map函数
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			line = value.toString();
			if ((line != null) && (!line.equals(""))) {

				String first = line.substring(0, line.indexOf("."));
				String day = line.substring(line.indexOf(".") + 1, line.length());
				try {
					if (day.length() == 1) {
						line = first + ".0" + day;
					}
					context.write(new Text(),new Text(line));
				} catch (Exception e) {
					System.out.println(line);
					e.printStackTrace();
				}

			}
		}

	}

	/**
	 * 
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] oArgs) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "lenovo0:9001");

		String[] ioArgs = new String[] { "/user/hadoop/alibaba/t_ali_data.txt", "/user/hadoop/alibaba/initial" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		System.out.println(otherArgs[0]);
		System.out.println(otherArgs[1]);
		Job job = new Job(conf, "InitialData");
		job.setJarByClass(InitialData.class);
		job.setMapperClass(Map.class);

		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 默认,可省略不写
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
