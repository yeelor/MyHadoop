package org.robby.mr.datatype;



import java.io.IOException;
import java.util.Iterator;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DataTypeMR extends Configured implements Tool {

	public static class Map extends
			Mapper<LongWritable, Text, SmCdr, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 192.168.1.101 - - [23/Jan/2013:22:07:21 +0800]
			// "GET /hadoop2/suggestion/sug.jsp?query=hello HTTP/1.1" 200 1294
			SmCdr sm = new SmCdr();
			sm.a = value.toString();
			sm.b = "b";
			sm.c = "2013";
			context.write(sm, new IntWritable(1));
		}
	}

	public static class Reduce extends
			Reducer<Text, SmCdr, SmCdr, SmCdr> {

		public void reduce(Text key, Iterator<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}

			//context.write(key, new IntWritable(sum));
		}
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int ret = ToolRunner.run(new DataTypeMR() , args);
		System.exit(ret);
	}
 
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();

		Job job = new Job(conf, "Load Redis");

		job.setJarByClass(DataTypeMR.class);

		job.setOutputKeyClass(SmCdr.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		//job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
