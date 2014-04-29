package com.j2cms.hadoop.mapreduce.alibaba;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 求从来没有过购买行为的用户，这部分用户没有购买力，应该被T除
 * 
 * @author aleak
 * 
 */
public class NeverBought {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		private static String line = new String();// 每行数据

		IntWritable one = new IntWritable();

		// 实现map函数
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			line = value.toString();
			if ((line != null) && (!line.equals(""))) {

				String values[] = line.split(",");// 1个或多个空格

				String userId = values[0];
				String brand = values[1];
				String type = values[2];
				String visitDate = values[3];

				context.write(new Text(userId), new Text(type));

			}

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		// 点击：0；购买：1；收藏：2；购物车：3
		int scores[] = { 1, 1000, 10, 100 };

		// 实现reduce函数
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean flag = false;
			for (Text v : values) {
				if (v.toString().equals("1"))// 假如有购买行为
					flag = true;
			}
			if (!flag)
				context.write(key, new Text());

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
		//
//		String date = new SimpleDateFormat("yyyyMMdd").format(new Date());
		String[] ioArgs = new String[] { "/user/hadoop/alibaba/t_data.txt", "/user/hadoop/alibaba/never_bought" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		System.out.println(otherArgs[0]);
		System.out.println(otherArgs[1]);
		Job job = new Job(conf, "never_bought");
		job.setJarByClass(NeverBought.class);
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
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
