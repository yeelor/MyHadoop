package com.j2cms.hadoop.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Score {

	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		
//		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
//			String line = value.toString();
//			StringTokenizer tokenizerArticle = new StringTokenizer(line,"\n");
//			
//			while (tokenizerArticle.hasMoreElements()){
//				StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
//				String strName = tokenizerLine.nextToken();
//				String strScore = tokenizerLine.nextToken();
//				
//				System.out.println("姓名:"+strName+" 分数:"+strScore);
//				
//				Text name = new Text(strName);
//				int score = Integer.parseInt(strScore);
//				
//				//
//				context.write(name, new IntWritable(score));
//			}
//		}
	
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String line = value.toString();
			StringTokenizer tokenizerLine = new StringTokenizer(line);
			String strName = tokenizerLine.nextToken();
			String strScore = tokenizerLine.nextToken();
			
			System.out.println("姓名:"+strName+" 分数:"+strScore); //并不能在控制台打印出来？！ why??
			
			Text name = new Text(strName);
			int score = Integer.parseInt(strScore);
			
			//
			context.write(name, new IntWritable(score));
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce (Text key ,Iterable<IntWritable>values,Context context) throws IOException,InterruptedException{
			int sum = 0 ;
			int count = 0;
			Iterator<IntWritable> iterator = values.iterator();
			while (iterator.hasNext()){
				sum+=iterator.next().get();
				count++;
			}
			int average = (int)sum/count;//计算平均成绩
			context.write(key, new IntWritable(average));
		}
	}

	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf  = new Configuration();
		conf.set("mapred.job.tracker", "192.168.162.128:9001");
		
		String[]ioArgs = new String[]{"score_in","score_out_"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())};
		String[]otherArgs = new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		if(otherArgs.length!=2){
			System.out.println("Usage:Score Average <in> <out>");
			System.out.equals(2);
		}
		Job job = new Job(conf,"Score Average");
		job.setJarByClass(Score.class);
		
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		//设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
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
