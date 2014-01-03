package com.j2cms.hadoop.mapreduce;



import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


/**
 *
A+b per line 

input:
1 200   314
2 2000  332
3 6000  333
4 6000  333
5 5000  333
6 30    12 

输出样例:

1 514
2 2332
3 6333
4 6333
5 5333
6 42

注意:
1 输入文件和输出文件都只有一个；
2 输入和输出文件每行的第一个数字都是行标；
3 每个数据都是正整数或者零.。

 *
 */
/**
 * A+B
 * @author GT
 *
 */
public class AplusB {
	
	public static class Map extends Mapper<Object,Text,Text,Text>{
		private static String  line = new String();//每行数据
		int firstBlank;
		String key;
		
		
		//实现map函数
		public void map(Object key,Text value ,Context context) throws IOException ,InterruptedException{
			line = value.toString();
//			String [] s = string.split(" ");//这是以空格拆分字符串
//			String [] s = string.split("\\s+");//1个或多个空格
//			String [] s = string.split("\\s|[\\s]|[^\\S]");//一个空格|竖线是或者的意思。
			String values[] = line.split("\\s+");//1个或多个空格
			
//			key  = line.substring(0, firstBlank);
			
			context.write(new Text(values[0]), new Text(String.valueOf(Integer.parseInt(values[1])+Integer.parseInt(values[2]))));
		}
	}
	
//	public static class Reduce extends Reducer<Text,Text,Text,Text>{
//		//实现reduce函数
//		public void reduce(Text key ,Iterable<Text> values ,Context context) throws IOException,InterruptedException{
//			context.write(key, new Text(""));
//		}
//	}

	/**
	 * 
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] oArgs) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration ();
		conf.set("mapred.job.tracker", "218.193.154.162:9001");
//		
		String[] ioArgs = new String[]{"AplusB","AplusB_out_"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())};
		String[] otherArgs = new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		System.out.println(otherArgs[0]);
		System.out.println(otherArgs[1]);
		Job job = new Job(conf,"A+B");
		job.setJarByClass(AplusB.class);
		job.setMapperClass(Map.class);
//		job.setCombinerClass(Reduce.class);
//		job.setReducerClass(Reduce.class);
		
		//设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//默认,可省略不写 
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}

