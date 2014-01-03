package com.j2cms.hadoop.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex {

	public static class Map extends Mapper<Object,Text,Text,Text>{
		
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();
		private FileSplit split;//存储Split对象
		
		public void map(Object key ,Text value,Context context) throws IOException,InterruptedException{
			split = (FileSplit) context.getInputSplit();
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreElements()){
				int splitIndex = split.getPath().toString().indexOf("file");
				keyInfo.set(itr.nextToken()+":"+split.getPath().toString().substring(splitIndex));
				
				valueInfo.set("1");
				context.write(keyInfo, valueInfo);
			}
		}
	}
	
	public static class Combine extends Reducer<Text,Text,Text,Text>{
		private Text info = new Text();
		
		public void reduce(Text key,Iterable<Text> values ,Context context)throws IOException,InterruptedException{
			int sum = 0;
			for(Text value:values){
				sum+=Integer.parseInt(value.toString());
			}
			int splitIndex = key.toString().indexOf(":");
			
			info.set(key.toString().substring(splitIndex+1)+":"+sum);
			key.set(key.toString().substring(0,splitIndex));
			
			context.write(key, info);
		}
	}
	
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		private Text result = new Text();
		
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
			String fileList = new String ();
			for(Text value:values){
				fileList +=value.toString()+";";
			}
			
			result.set(fileList);
			context.write(key, result);
			
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf  = new Configuration();
		conf.set("mapred.job.tracker", "192.168.162.128:9001");
		
		String[]ioArgs = new String[]{"index_in","index_out_"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())};
		String[]otherArgs = new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		if(otherArgs.length!=2){
			System.out.println("Usage:Inverted Index <in> <out>");
			System.out.equals(2);
		}
		Job job = new Job(conf,"Inverted Index");
		job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Combine.class);//这里有了
		job.setReducerClass(Reduce.class);
		
		//设置输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//将输入的数据分割成小数据块splites,提供一个RecordReader的实现
		job.setInputFormatClass(TextInputFormat.class);
		//提供一个RecordWriter的实现，负责数据输出
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//设置输入的输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
