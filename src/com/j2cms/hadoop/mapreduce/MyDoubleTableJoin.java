package com.j2cms.hadoop.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
import org.mortbay.log.Log;

/**
 * 多表关联
 * @author GT 2013.10.29
 *
 */
public class MyDoubleTableJoin {

	public static int time = 0;

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			if(line.startsWith("addressID")||line.startsWith("factoryName"))
				return ;
			char firstChar =line.charAt(0);
			if(firstChar>='0'&&firstChar<='9'){//是地址编号表，作为右表
				String addressID = String.valueOf(firstChar);
				String addressName =line.substring(line.lastIndexOf(" ")+1);
				context.write(new Text(addressID), new Text("R#"+addressName));
			}else{//是工厂编号表，作为左表
				int lastBlank = line.lastIndexOf(" ");
				String factoryName = line.substring(0, lastBlank);
				String addressID = line.substring(lastBlank+1);
				context.write(new Text(addressID),new Text("L#"+factoryName));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if(0==time) {
				context.write(new Text("factoryName"), new Text("addressName"));//输出文件的第一行
				time++;
			}
				
			List<String> factoryList = new ArrayList<String>();
			List<String> addressList = new ArrayList<String>();
			
			for (Text value : values) {
				String record = value.toString();
				char type =record.charAt(0);
				Log.info("type="+type);
				if(type=='L'){
					factoryList.add(record.substring(2));
				}else if(type=='R'){
					addressList.add(record.substring(2));
				}				
			}
			for(String factory:factoryList){
				for(String address:addressList){
					context.write(new Text(factory), new Text(address));
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
		String []ioArgs = new String []{"join_in_double","join_out_double_"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())};
		String[] otherArgs = new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		
		Job job = new Job(conf,"Double Table join");
		job.setJarByClass(MyDoubleTableJoin.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
