package org.robby.mr.pagerank;

import org.apache.commons.io.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.*;

public final class Main {

	private static Configuration conf = new Configuration();

	/**
	 * 迭代
	 * @param input
	 * @param output
	 * @throws Exception
	 */
	public static void iterate(String input, String output) throws Exception {

		// 删除输出目录
		Path outputPath = new Path(output);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		//这一步好像不是必须的
		outputPath.getFileSystem(conf).mkdirs(outputPath);

		Path inputPath = new Path(outputPath, "input.txt");

		int numNodes = createInputFile(new Path(input), inputPath);

		int iter = 1;
		double desiredConvergence = 0.01;

		while (true) {

			Path jobOutputPath = new Path(outputPath, String.valueOf(iter));

			System.out.println("======================================");
			System.out.println("=  Iteration:    " + iter);
			System.out.println("=  Input path:   " + inputPath);
			System.out.println("=  Output path:  " + jobOutputPath);
			System.out.println("======================================");

			if (calcPageRank(inputPath, jobOutputPath, numNodes,iter) < desiredConvergence) {
				System.out.println("Convergence is below " + desiredConvergence + ", we're done");
				break;
			}
			inputPath = jobOutputPath;
			iter++;
		}
	}

	/**
	 * 创建初始始化的文件.在output目录生成第一个文件output/input.txt,作为第一次迭代的输入文件
	 * @param file
	 * @param targetFile
	 * @return
	 * @throws IOException
	 */
	public static int createInputFile(Path file, Path targetFile) throws IOException {

		FileSystem fs = file.getFileSystem(conf);

		int numNodes = getNumNodes(file);
		double initialPageRank = 1.0 / (double) numNodes;

		OutputStream os = fs.create(targetFile);
		LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");

		while (iter.hasNext()) {
			String line = iter.nextLine();

			if ((line != null) && (!line.equals(""))) {
				String[] parts = StringUtils.split(line);

				Node node = new Node(initialPageRank);
				node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 1, parts.length));
				IOUtils.write(parts[0] + '\t' + node.toString() + '\n', os);
			}

		}
		os.close();
		return numNodes;
	}

	/**
	 * 返回接点个数，也就是文件的行数
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public static int getNumNodes(Path file) throws IOException {

		FileSystem fs = file.getFileSystem(conf);

		return IOUtils.readLines(fs.open(file), "UTF8").size();
	}

	/**
	 * 提交到master计算pageRank
	 * @param inputPath
	 * @param outputPath
	 * @param numNodes
	 * @return  阀值
	 * @throws Exception
	 */
	public static double calcPageRank(Path inputPath, Path outputPath, int numNodes,int iter) throws Exception {
		
		Configuration conf = new Configuration();
		
		conf.set("mapred.job.tracker", "lenovo0:9001");
		
		conf.setInt(Reduce.CONF_NUM_NODES_GRAPH, numNodes);

		Job job = new Job(conf, "page_rank_"+iter);
		job.setJarByClass(Main.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// 比较特殊,key value
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		if (!job.waitForCompletion(true)) {
			throw new Exception("Job failed");
		}

		long summedConvergence = job.getCounters().findCounter(Reduce.Counter.CONV_DELTAS).getValue();
		double convergence = ((double) summedConvergence / Reduce.CONVERGENCE_SCALING_FACTOR) / (double) numNodes;

		System.out.println("======================================");
		System.out.println("=  Num nodes:           " + numNodes);
		System.out.println("=  Summed convergence:  " + summedConvergence);
		System.out.println("=  Convergence:         " + convergence);
		System.out.println("======================================");

		return convergence;
	}

	public static void main(String... args) throws Exception {

		
		
		String inputFile = args[0];
		String outputDir = args[1];

		iterate(inputFile, outputDir);
	}

}
