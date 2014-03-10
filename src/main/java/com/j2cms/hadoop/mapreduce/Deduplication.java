package com.j2cms.hadoop.mapreduce;

import java.io.IOException;

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
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 去重
 * @author hadoop
 *
 */
public class Deduplication {
	
	public static class Map extends Mapper<Object,Text,Text,Text>{
		private static Text line = new Text();//每行数据
		
		//实现map函数
		public void map(Object key,Text value ,Context context) throws IOException ,InterruptedException{
			line = value;
			context.write(line, new Text(""));
		}
	}
	
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		//实现reduce函数
		public void reduce(Text key ,Iterable<Text> values ,Context context) throws IOException,InterruptedException{
			context.write(key, new Text(""));
		}
	}

	/**
	 * 
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration ();
		conf.set("mapred.job.tracker", "192.168.162.128:9001");
		
		String[] ioArgs = new String[]{"dedup_in","dedup_out"};
		String[] otherArgs = new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		Job job = new Job(conf,"Data deduplication");
		job.setJarByClass(Deduplication.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		//设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//默认,可省略不写 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
