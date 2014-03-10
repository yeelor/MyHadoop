package com.j2cms.hadoop.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//利用MapReduce求最大值海量数据中的K个数
public class TopKNum extends Configured implements Tool {
	
	public static class MapClass extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		public static final int K = 10;
		private int[] top = new int[K];

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();//去掉空格
			line  = line.trim();
			if((line!=null)&&(!line.equals(""))){
				int temp = Integer.parseInt(line);
				add(temp);	
			}
		}

		/**
		 * 维持一个topK的数组
		 * @param temp
		 */
		private void add(int temp) {
			if (temp > top[0]) {
				top[0] = temp;
				int i = 0;
				for (; i < (K-1) && temp > top[i + 1]; i++) {
					top[i] = top[i + 1];
				}
				top[i] = temp;
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (int i = 0; i < K; i++) {
				context.write(new IntWritable(top[i]), new IntWritable(top[i]));
			}
		}
	}

	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public static final int K = 10;
		private int[] top = new int[K];

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			for (IntWritable val : values) {
				add(val.get());
			}
		}

		private void add(int temp) {// 实现插入if(temp>top[0]){
			top[0] = temp;
			int i = 0;
			for (; i < (K-1) && temp > top[i + 1]; i++) {
				top[i] = top[i + 1];
			}
			top[i] = temp;
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (int i = 0; i < K; i++) {
				context.write(new IntWritable(i), new IntWritable(top[i]));
			}
		}

	}

	public int run(String[] args) throws Exception {
		
	
		Configuration conf = getConf();
		Job job = new Job(conf, "TopKNum");
		job.setJarByClass(TopKNum.class);
		
		String []ioArgs = new String[]{"sort_in","topk_out_"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())};
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
		
		
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TopKNum(), args);
		System.exit(res);
	}
}
