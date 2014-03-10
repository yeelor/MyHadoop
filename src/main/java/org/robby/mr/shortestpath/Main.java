package org.robby.mr.shortestpath;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.commons.io.*;
import org.apache.commons.lang.*;
import org.robby.mr.count.RedisOutputFormat;


import java.io.*;


public class Main {

	public static final String TARGET_NODE = "shortestpath.targetnode";
	
	public static void createInputFile(Path file, Path targetFile, String startNode) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = file.getFileSystem(conf);
		
		DataOutputStream os = fs.create(targetFile);
		
		LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");
		while(iter.hasNext()){
			String line = iter.nextLine();
			
			String[] parts = StringUtils.split(line);
			int distance = Integer.MAX_VALUE;
			if(startNode.equals(parts[0])){
				distance = 0;
			}
			
			IOUtils.write(parts[0] + '\t' + String.valueOf(distance) + "\t\t", os);//有两个\t是因为要放上一个结点backpoint
			IOUtils.write(StringUtils.join(parts, '\t', 1, parts.length), os);
			IOUtils.write("\n", os);
		}
		os.close();
	}
	
	public static boolean findShortestPath(Path inputPath, Path outputPath, String startNode, String targetNode) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "lenovo0:9001");
		conf.set(TARGET_NODE, targetNode);
		
		//拷贝的方式传入的一个conf，所以conf在上面一步要设置
		Job job = new Job(conf);
		
		job.setJarByClass(Main.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
		
		Counter counter = job.getCounters().findCounter(Reduce.PathCounter.TARGET_NODE_FOUND);
		if(counter != null && counter.getValue() > 0){
			System.out.println("reduce return true");
		    return true;
		}
		
		return false;
	}
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		String startNode = args[0];
		String targetNode = args[1];
		//输入文件
		String inputFile = args[2];
		//输出目录
		String outputDir = args[3];
		
		System.out.println("targeNode=" + targetNode);
		
		Configuration conf = new Configuration();
		Path outputPath = new Path(new Path(outputDir), "input.txt");
		
		createInputFile(new Path(inputFile), outputPath, startNode);
		
		//
		//findShortestPath(outputPath, new Path("/output1"), startNode, targetNode);
		Path jobInput, jobOutput;
		jobInput = outputPath;
		int iter = 1;
		
		while(true){
			jobOutput = new Path(new Path(outputDir), String.valueOf(iter));
			
			if(findShortestPath(jobInput, jobOutput, startNode, targetNode)){
				break;
			}
			
			jobInput = jobOutput;
			iter ++;
		}
	}
}
