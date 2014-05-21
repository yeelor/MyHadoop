package com.j2cms.hadoop.mapreduce;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


class App{
	public static int K = 50;
} 

/**
 * 实例类
 * @author aleak
 *
 */
class KV implements Comparable<KV> {
	
	//ID
	public Text key;
	
	//进行比较的属性
	public int value;
	
	//可以再加其它属性,如果属性比较多，实现一个类的硬拷贝比较好

	public KV(Text key, int value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public int compareTo(KV o) {
		
		if(this.value> o.value)
			return 1;
		else 
			return -1;
//		return 0;
	}
}

// 利用MapReduce求海量数据中的最大的K个实例
public class TopK extends Configured implements Tool {

	public static class MapClass extends Mapper<Text, Text, Text, IntWritable> {
		private KV[] top = new KV[App.K];

		protected void setup(Context context) throws IOException, InterruptedException {
			for (int i = 0; i < App.K; i++) {
				top[i] = new KV(new Text(), 0);
//				System.out.println(top[i].key+"_"+top[i].value);
			}
		}

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			if (!key.toString().equals("")) {
				int v = Integer.valueOf(value.toString().trim());
				KV kv = new KV(key, v);
				
//				System.out.println(kv.key+"_"+kv.value);
				
				add(kv);
			}
		}

		/**
		 * 维持一个topK的数组
		 * @param temp
		 */
		private void add(KV temp) {
			if (temp.compareTo(top[0])>0) {
				top[0] = temp;
				top[0].key =new Text(temp.key);
				int i = 0;
				for (; (i < (App.K-1) )&& (temp.compareTo(top[i+1])>0); i++) {
					top[i] = top[i + 1];
					top[i].key =new Text(top[i + 1].key);
				}
				top[i] = temp;
				top[i].key = new Text(temp.key);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (int i = 0; i < App.K; i++) {
				
				context.write(top[i].key, new IntWritable(top[i].value));
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		private KV[] top = new KV[App.K];

		protected void setup(Context context) throws IOException, InterruptedException {
			for (int i = 0; i < App.K; i++) {
				top[i] = new KV(new Text(), 0);

			}
		}

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			for (IntWritable val : values) {
				KV kv = new KV(key, val.get());

				add(kv);
			}
		}

		/**
		 * 维持一个topK的数组
		 * @param temp
		 */
		private void add(KV temp) {
			if (temp.compareTo(top[0])>0) {
				top[0] = temp;
				top[0].key =new Text(temp.key);
				int i = 0;
				for (; (i < (App.K-1) )&& (temp.compareTo(top[i+1])>0); i++) {
					top[i] = top[i + 1];
					top[i].key =new Text(top[i + 1].key);
				}
				top[i] = temp;
				top[i].key = new Text(temp.key);
			}
		}



		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (int i = 0; i < App.K; i++) {
				context.write(top[i].key, new IntWritable(top[i].value));
			}
		}

	}

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		conf.set("mapred.job.tracker", "218.193.154.181:9001");
		Job job = new Job(conf, "TopKNum");
		job.setJarByClass(TopKNum.class);

		String[] ioArgs = new String[] { "/user/peter/drug_out", "/user/peter/drug_top/" + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) };
		String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TopKNum(), args);
		System.exit(res);
	}
}