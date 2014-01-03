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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {
	
	public static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()){
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			int sum = 0;
			for(IntWritable val:values){
				sum+= val.get();
			}
			result.set(sum);
			context.write(key, result);
		} 
	}
	
	public static void main(String [] args) throws Exception{
		
		long startTime = System.currentTimeMillis();
		 
		Configuration conf  = new Configuration();
		conf.set("mapred.job.tracker", "super:9001");
		String []ars = new String []{"/user/hadoop/input","/user/hadoop/wordcount_output_"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())};
		String[] otherArgs = new GenericOptionsParser(conf,ars).getRemainingArgs();
		if(otherArgs.length!=2){
			System.out.println("Usage:wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf,"word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//		System.exit(job.waitForCompletion(true)?0:1);
		job.waitForCompletion(true);
		
		long endTime = System.currentTimeMillis();
		System.out.println("time cost"+(endTime-startTime));
		
	}

}
