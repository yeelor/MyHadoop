package com.j2cms.hadoop.mapreduce.alibaba;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 A+b per line 

 input:
 1 200   314
 2 2000  332
 3 6000  333
 4 6000  333
 5 5000  333
 6 30    12 

 输出样例:

 1 514
 2 2332
 3 6333
 4 6333
 5 5333
 6 42

 注意:
 1 输入文件和输出文件都只有一个；
 2 输入和输出文件每行的第一个数字都是行标；
 3 每个数据都是正整数或者零.。

 *
 */
/**
 * A+B
 * 
 * @author GT
 * 
 */
public class T2 {

	public static class Map extends Mapper<Text, Text, Text, Text> {

		static int score = 0;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			score = Integer.valueOf(context.getConfiguration().get("score"));
		}

		// 实现map函数
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			String k = key.toString();
			if ((k != null) && (!k.equals(""))) {
				String values[] = k.split("_");// 1个或多个空格
				String userId = values[0];
				String brand = values[1];

				System.out.println("score=" + score);

				if (Integer.valueOf(value.toString()) >= score) {
					context.write(new Text(userId), new Text(brand));
				}
			}

		}
	}

	// 将每个用户将要购买的品牌串起来
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		// 注意，在数据量比较小时，只有一个reduce，这里的统计才是正确的.
		int pBrand = 0;

		// 实现reduce函数
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String s = "";
			for (Text v : values) {
				s += v.toString() + ",";
				pBrand++;
			}
			if (s.endsWith(","))
				s = s.substring(0, s.length() - 1);
			context.write(key, new Text(s));
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("pBrand="+pBrand);
			context.getConfiguration().set("pBrand", String.valueOf(pBrand));
		}
	}

	public static void job(String input,String output,String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "lenovo0:9001");
		conf.set("score", args[0]);

		Job job = new Job(conf,"T2_"+ args[0] + "_" + args[1] + "_result");
		job.setJarByClass(T2.class);
		job.setMapperClass(Map.class);
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 默认,可省略不写
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
	}

}
