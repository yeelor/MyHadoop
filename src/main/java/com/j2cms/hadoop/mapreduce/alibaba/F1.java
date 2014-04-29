package com.j2cms.hadoop.mapreduce.alibaba;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;

public class F1 {

	public static class Map extends Mapper<Text, Text, Text, Text> {

	
		// 实现map函数
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			if ((key != null) && (!key.equals(""))) {
				String vs[] = value.toString().split(",");
				for(String v:vs){
					context.write(key, new Text(v));
				}
			}
		}
	}

	// 将每个用户将要购买的品牌串起来
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public static enum Counter {
			P_BRAND,HIT
		}

		// 注意，在数据量比较小时，只有一个reduce，这里的统计才是正确的.
		int pBrand = 0;
		
		int hit = 0;

		
		Set<String> boughtSet = new HashSet<String>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Path inPath =new Path(Aconst.rootPath + "bought/bought_once_7.16-8.15.txt");
			Configuration conf = new Configuration();
			
			FileSystem hdfs = FileSystem.get(conf); // FileSystem是用户操作HDFS的核心类，它获得URI对应的HDFS文件系统
			FSDataInputStream dis = hdfs.open(inPath);
			LineReader in = new LineReader(dis, conf);
			Text line = new Text();
			// 按行读取
			while (in.readLine(line) > 0) {
				boughtSet.add(line.toString().replace("\t", "_"));
//				System.out.println(line.toString());
			}
			dis.close();
			in.close();
		}

	
		// 实现reduce函数
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String ub = new String();
			for (Text v : values) {
				pBrand++;
				ub = key.toString()+"_"+v.toString();
				if(boughtSet.contains(ub)){
					hit++;//命中
					context.write(key, v);
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("pBrand=" + pBrand);
			context.getCounter(Counter.P_BRAND).increment(pBrand);
			context.getCounter(Counter.HIT).increment(hit);
		}
	}

	public static String job(String input, String output) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", Aconst.master);

		Job job = new Job(conf, "F1-P-R-" + input.substring(input.indexOf(Aconst.date)) + "-[" + new SimpleDateFormat("yyyyMMdd HH:mm").format(new Date()) + "]");
		job.setJarByClass(F1.class);
		job.setMapperClass(Map.class);
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 默认,可省略不写
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);

		long pBrand = job.getCounters().findCounter(Reduce.Counter.P_BRAND).getValue();
		long hit = job.getCounters().findCounter(Reduce.Counter.HIT).getValue();
		float p = (float)hit/(float)pBrand;
		float r = (float)hit/(float)Aconst.bBrand;
		float f1 = 2*p*r/(p+r);
		String otp ="\n"+job.getJobName() + "\n" + "pBrand=" + pBrand+"\nPRECISION="+p+"\tRECALL="+r+"\tF1="+f1;

		System.out.println(otp);
		return otp;
	}

}
