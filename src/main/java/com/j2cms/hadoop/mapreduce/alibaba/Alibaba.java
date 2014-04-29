package com.j2cms.hadoop.mapreduce.alibaba;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Alibaba extends Configured implements Tool {
	
	public static class MapClass extends Mapper<Text, Text, Text, Text> {
		
		
		Text k = new Text();
		IntWritable v = new IntWritable();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();//去掉空格
			line  = line.trim();
			if((line!=null)&&(!line.equals(""))){
				String[] us =line.split(",");
				for(String s:us){
					if((s!=null)&&(!s.equals("")))
						context.write(key,new Text(s));
				}
				
			}
		}
	}

//	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
//	
//
//
//		IntWritable v = new IntWritable();
//		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//			int score =0;
//			for (IntWritable val : values) {
//				System.out.println("key="+key+"val="+val.get());
//				score+=val.get();
//			}
//			v.set(score);
//			
//			context.write(key, v);
//			
//		}
//	}

	public int run(String[] args) throws Exception {
		
	
		Configuration conf = getConf();
		conf.set("mapred.job.tracker", "lenovo0:9001");
		
		Job job = new Job(conf, "A1");
		job.setJarByClass(Alibaba.class);
		
		String []ioArgs = new String[]{"/user/hadoop/alibaba/400_contains_70.txt","/user/hadoop/alibaba/400-70"};
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		
		
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setMapperClass(MapClass.class);
//		job.setCombinerClass(Reduce.class);
//		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Alibaba(), args);
		System.exit(res);
	}
}
