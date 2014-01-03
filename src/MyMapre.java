

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * A+B
 * @author hadoop
 *
 */
public class MyMapre {
	
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
	
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		//实现reduce函数
		public void reduce(Text key ,Iterable<Text> values ,Context context) throws IOException,InterruptedException{
			for(Text value:values){
				context.write(key, value);
			}
			
		}
	}

	/**
	 * 
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] orgs) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration ();
		conf.set("mapred.job.tracker", "192.168.162.128:9001");
		
		String[] ioArgs = new String[]{"AplusB","AplusB_out_"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())};
		String[] otherArgs = new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		Job job = new Job(conf,"AplusB");
		job.setJarByClass(MyMapre.class);
		job.setMapperClass(Map.class);
//		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
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
