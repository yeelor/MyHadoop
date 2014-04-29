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

public class TdataOrderByDate {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		private static String line = new String();// 每行数据

		private static float firstDay = (float) 12.31;
		private static float lastDay = (float) 1.1;

		// 实现map函数
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			line = value.toString();
			if ((line != null) && (!line.equals(""))) {
				String values[] = line.split(",");// 1个或多个空格
				// String userId = values[0];
				// String brand = values[1];
				// String type = values[2];
				float visitDay = Float.valueOf(values[3]);
				if (visitDay < firstDay)
					firstDay = visitDay;
				if (visitDay > lastDay)
					lastDay = visitDay;
				context.write(new Text(values[3]), new Text(values[0] + "," + values[1] + "," + values[2]));
			}
		}
		
		@Override
		protected void cleanup(Mapper<Object,Text,Text,Text>.Context context) throws IOException ,InterruptedException {
			System.out.println("firstDay="+firstDay);
			System.out.println("lastDay="+lastDay);
			
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
//		String date = new SimpleDateFormat("yyyyMMdd").format(new Date());
		String[] ioArgs = new String[] { "/user/hadoop/alibaba/t_data.txt", "/user/hadoop/alibaba/t_data_order_by_date" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		System.out.println(otherArgs[0]);
		System.out.println(otherArgs[1]);
		Job job = new Job(conf, "t_data_order_by_date");
		job.setJarByClass(TdataOrderByDate.class);
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
