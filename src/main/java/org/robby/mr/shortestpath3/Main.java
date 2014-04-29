package org.robby.mr.shortestpath3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.io.*;
import org.apache.commons.lang.*;


import redis.clients.jedis.Jedis;

import java.io.*;

public class Main {

	public static final String maxInt = String.valueOf(Integer.MAX_VALUE);//2147483647
	
	/**
	 * 将data.txt里的数据保存到redis数据库
	 * @param file
	 * @throws Exception
	 */
	public static void createInputFile(Path file) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = file.getFileSystem(conf);
		
		
		/**
		 * 远程redis服务器要开6379端口
		 */
		Jedis jedis = new Jedis("218.193.154.135");
		LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");
		while(iter.hasNext()){
			String line = iter.nextLine();
			
			String[] parts = StringUtils.split(line);
			jedis.hset("ALL_NODE",parts[0],maxInt);
			
			
			
//		    jedis.lpush("all_nodes", parts[0]);
		    
			String nodeId = "node_" + parts[0];
			System.out.println("length" + parts.length);
			if(parts.length > 1){
				for(int i=1; i<parts.length; i++){
					jedis.lpush(nodeId, parts[i]);
				}
			}
		}
		jedis.disconnect();
	}
	
	public static void findShortestPath(Path inputPath, Path outputPath) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "lenovo0:9001");
			
		Job job = new Job(conf,"shortest_path3");
	
		job.setJarByClass(Main.class);
		job.setMapperClass(FindShortestMap.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);

		
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		String inputFile = args[0];
		String outputDir = args[1];

		
		System.out.println("arg0=" + args[0]);
		System.out.println("arg1=" + args[1]);
		
		//删除output
		Configuration conf = new Configuration();
		Path outputPath = new Path(outputDir);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		

		createInputFile(new Path(inputFile));
//		findShortestPath(new Path("/input/"), new Path("/output/"));
		findShortestPath(new Path(inputFile), new Path(outputDir));
	}

}
