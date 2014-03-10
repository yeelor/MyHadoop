package org.robby.mr.count;

import java.io.IOException;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 192.168.1.101 - - [23/Jan/2013:22:07:21 +0800]
			// "GET /hadoop2/suggestion/sug.jsp?query=hello HTTP/1.1" 200 1294
			String line = value.toString();

			String a[] = line.split("\"");
			if (a[1].indexOf("sug.jsp?query") > 0) {
				String b[] = a[1].split("query=| ");
				
				String tmp =URLDecoder.decode(b[2], "UTF-8");
				
				Text word = new Text(tmp);
				context.write(word, one);
			}
		}

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val:values){
				sum+= val.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}


	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = super.getConf();
		Job job = new Job(conf, "Load Redis");
		
		DistributedCache.addFileToClassPath(new Path("/home/hadoop/Downloads/jedis-2.4.1.jar"), conf);
		
		job.setJarByClass(WordCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(RedisOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		RedisOutputFormat.setOutputPath(job, new Path(args[1]));
		String[] ioArgs = new String[]{"logs","output_"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())};
		RedisOutputFormat.setOutputPath(job, new Path(ioArgs[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String args[]) throws Exception{
		int ret = ToolRunner.run(new WordCount(), args);
		System.exit(ret);
	}
	

}
