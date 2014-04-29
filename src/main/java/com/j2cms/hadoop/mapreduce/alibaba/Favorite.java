package com.j2cms.hadoop.mapreduce.alibaba;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class Favorite extends Configured implements Tool {

	public static class MapClass extends Mapper<Text, Text, Text, Text> {

		Text k = new Text();
		Text v = new Text();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

			String keyString = key.toString();
			String[] us = keyString.split("_");
			Integer v = Integer.valueOf(value.toString());
//			if((v>=100)&&(v<1000))
//			if(v>=3)
//			if(v>=10)
			if(v>=100)
//			if(v>=1000)
			context.write(new Text(us[0]) , new Text(us[1]));

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		Text v = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if((key!=null)&&(!key.equals(new Text("")))){
				String v ="";
				for (Text val : values) {
					if((val!=null)&&(!val.equals(new Text(""))))
					v+=val.toString()+",";
				}
				if(v.endsWith(","))
					v=v.substring(0,v.length()-1);
				
				context.write(key, new Text(v));
			}
			

		}

	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		conf.set("mapred.job.tracker", "218.193.154.181:9001");

		Job job = new Job(conf, "F");
		job.setJarByClass(Favorite.class);

		String[] ioArgs = new String[] { "/user/hadoop/alibaba/output3", "/user/hadoop/alibaba/>=100" };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setMapperClass(MapClass.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Favorite(), args);
		System.exit(res);
	}
}
