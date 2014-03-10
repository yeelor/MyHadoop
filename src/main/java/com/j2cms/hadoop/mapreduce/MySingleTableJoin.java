package com.j2cms.hadoop.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 单表自关联
 * @author GT 2013.10.29
 *
 */
public class MySingleTableJoin {

	public static int time = 0;

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer stk = new StringTokenizer(value.toString());
			String[] values = new String[2];
			values[0] = stk.nextToken();
			values[1] = stk.nextToken();
			if (!values[0].equals("child")) {
				context.write(new Text(values[1]), new Text("L#" + values[0]));// 左表
				context.write(new Text(values[0]), new Text("R#" + values[1]));// 右表
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if(0==time) {
				context.write(new Text("grandchild"), new Text("grandparent"));//输出文件的第一行
				time++;
			}
				
			List<String> grandchildList = new ArrayList<String>();
			List<String> grandparentList = new ArrayList<String>();
			
			for (Text value : values) {
				String record = value.toString();
				char type =record.charAt(0);
				System.out.println("type="+type);
				if(type=='L'){
					grandchildList.add(record.substring(2));
				}else if(type=='R'){
					grandparentList.add(record.substring(2));
				}				
			}
			for(String grandchild:grandchildList){
				for(String grandparent:grandparentList){
					context.write(new Text(grandchild), new Text(grandparent));
				}
			}
		}

	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "master:9001");
		String []ioArgs = new String []{"join_in","join_out_"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())};
		String[] otherArgs = new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		
		Job job = new Job(conf,"Single Table join");
		job.setJarByClass(MySingleTableJoin.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
