package com.j2cms.hadoop.mapreduce.alibaba;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class F0 {

	public static class Map extends Mapper<Text, Text, Text, Text> {

		static int score = 0;

		// 实现map函数
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			if ((key != null) && (!key.equals(""))) {
				context.write(key, value);
			}

		}
	}

	// 将每个用户将要购买的品牌串起来
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public static enum Counter {
			P_BRAND
		}

		// 注意，在数据量比较小时，只有一个reduce，这里的统计才是正确的.
		int pBrand = 0;

		// 实现reduce函数
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Set<String> brands = new HashSet<String>();
			for (Text v : values) {
				brands.add(v.toString());
			}

			String s = "";
			for (String v : brands) {
				s += v + ",";
				pBrand++;
			}
			if (s.endsWith(","))
				s = s.substring(0, s.length() - 1);
			context.write(key, new Text(s));
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("pBrand=" + pBrand);
			context.getConfiguration().set("pBrand", String.valueOf(pBrand));
			// System.out.println("pBrand=" +
			// context.getConfiguration().get("pBrand"));
			context.getCounter(Counter.P_BRAND).increment(pBrand);
		}
	}

	public static String job(List<String> inputs, String output, List<String> plus) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", Aconst.master);

		Job job = new Job(conf, "F0-" + output.substring(output.indexOf(Aconst.date)) + "-[" + new SimpleDateFormat("yyyyMMdd HH:mm").format(new Date()) + "]");
		job.setJarByClass(F0.class);
		job.setMapperClass(Map.class);
		// job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		// 设置输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 默认,可省略不写
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 设置输入目录，可能有多个
		for (String input : inputs) {
			FileInputFormat.addInputPath(job, new Path(input));
		}

		// 设置输入目录，可能有多个
		for (String input : plus) {
			FileInputFormat.addInputPath(job, new Path(input));
		}

		// 输出目录
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);

		long pBrand = job.getCounters().findCounter(Reduce.Counter.P_BRAND).getValue();
		String otp = job.getJobName() + "\n" + "pBrand=" + pBrand;
		System.out.println(otp);
		return otp;
	}

	public static String job(List<String> inputs, String output, String plus) throws Exception {
		List<String> ps = new ArrayList<String>();
		ps.add(plus);
		return F0.job(inputs, output, ps);
	}

	public static String job(List<String> inputs, String output) throws Exception {
		return F0.job(inputs, output, new ArrayList<String>());
	}

	/**
	 * 
	 * @param args
	 *            [0] 分数 args[1]日期
	 * @param target
	 *            T3的结果移动到的目录
	 * @throws Exception
	 */
	public static void runT1AndT3(String args[]) throws Exception {
		// 输入输出目录
		String ouput4T3 = Aconst.rootPathPlusWs + args[0] + "_" + args[1];
		if (!HDFSUtil.exists(ouput4T3)) {// 因为前面的操作可能生成过相同的文件
			String output4T1 = Aconst.rootPathPlusWs + args[0] + "_" + args[1] + "_us";// user_score
			if (HDFSUtil.exists(output4T1))
				HDFSUtil.deletePath(output4T1);
			T1.job(Aconst.aliFile, output4T1, args);
			HDFSUtil.deleteNoUse(output4T1);

			T3.job(output4T1, ouput4T3, args);
			HDFSUtil.deleteNoUse(ouput4T3);

			HDFSUtil.deletePath(output4T1);
		}
	}

	/**
	 * 将多个文件合并成最终结果
	 * 
	 * @param sdsList
	 * @throws Exception
	 */
	public static String merge(List<String[]> sdsList, String outputFolder) throws Exception {
		String[][] sdss = (String[][]) sdsList.toArray(new String[0][0]);
		return merge(sdss, outputFolder);
	}

	/**
	 * 将多个文件合并成最终结果
	 * 
	 * @param sd
	 *            [0][0] 分数 sd[0][1]开始日期
	 * @throws Exception
	 */
	public static String merge(String sdss[][], String outputFolder) throws Exception {

		StringBuffer outputPrint = new StringBuffer();

		if (outputFolder == null) {
			outputFolder = Aconst.rootPathPlusDatePlusWs;
			for (int i = 0; i < sdss.length; i++) {
				// 生成形如 3_8.10_5_8.01_ 的格式
				outputFolder += sdss[i][0] + "_" + sdss[i][1] + "_";
			}
			outputFolder.substring(0, outputFolder.length() - 1);// 去掉最后一个 _
		}
		// 多个输入文件
		List<String> inputs = new ArrayList<String>();

		List<String> is = new ArrayList<String>();

		for (int i = 0; i < sdss.length; i++) {
			inputs.add(Aconst.rootPathPlusWs + sdss[i][0] + "_" + sdss[i][1]);
			is.add(Aconst.ws + "/" + sdss[i][0] + "_" + sdss[i][1]);
		}
		HDFSUtil.write(outputFolder + "/input.txt", StringUtils.join(is, "\n"));

		for (int i = 0; i < sdss.length; i++) {
			F0.runT1AndT3(sdss[i]);
		}

		String output = "";
		String plus = "";
		String opt = "";

		output = outputFolder + "/F0";
		opt = F0.job(inputs, output);
		HDFSUtil.deleteNoUse(output);
		// opt+=F1.job(output, output+"_HIT");//计算F1值
		// HDFSUtil.deleteNoUse(output+"_HIT");
		outputPrint.append(opt + "\n");

		// 加上一部分数据，再做一次
		output = outputFolder + "/F0_PLUS_twice";
		plus = Aconst.rootPath + "bought/bought_or_cart_twice.txt";
		// plus = Aconst.rootPath + "bought/bought_or_cart_twice_4.15-7.15.txt";
		opt = F0.job(inputs, output, plus);
		HDFSUtil.deleteNoUse(output);
		// opt+=F1.job(output, output+"_HIT");//计算F1值
		// HDFSUtil.deleteNoUse(output+"_HIT");
		outputPrint.append(opt + "\n");

		// 加上一部分数据，再做一次
		output = outputFolder + "/F0_PLUS_once_7.15-8.15";
		plus = Aconst.rootPath + "bought/bought_once_7.15-8.15.txt";
		// output = outputFolder + "/F0_PLUS_once";
		// plus = Aconst.rootPath + "bought/bought_once_6.16-7.15.txt";
		opt = F0.job(inputs, output, plus);
		HDFSUtil.deleteNoUse(output);
		// opt+=F1.job(output, output+"_HIT");//计算F1值
		// HDFSUtil.deleteNoUse(output+"_HIT");
		outputPrint.append(opt + "\n");

		// 加上一部分数据，再做一次
		output = outputFolder + "/F0_PLUS_once_twice";
		List<String> ps = new ArrayList<String>();
		ps.add(Aconst.rootPath + "bought/bought_or_cart_twice.txt");
		ps.add(Aconst.rootPath + "bought/bought_once_7.15-8.15.txt");
		// ps.add(Aconst.rootPath +
		// "bought/bought_or_cart_twice_4.15-7.15.txt");
		// ps.add(Aconst.rootPath + "bought/bought_once_6.16-7.15.txt");
		opt = F0.job(inputs, output, ps);
		HDFSUtil.deleteNoUse(output);
		// opt+=F1.job(output, output+"_HIT");//计算F1值
		// HDFSUtil.deleteNoUse(output+"_HIT");
		outputPrint.append(opt + "\n");
		//
		// // 加上一部分数据，再做一次
		// output = outputFolder + "/F0_PLUS_once_m22";
		// ps = new ArrayList<String>();
		// ps.add(Aconst.rootPath + "bought/m22.txt");
		// ps.add(Aconst.rootPath + "bought/bought_once_7.15-8.15.txt");
		// // ps.add(Aconst.rootPath + "bought/m22-415715.txt");
		// // ps.add(Aconst.rootPath + "bought/bought_once_6.16-7.15.txt");
		// opt=F0.job(inputs, output, ps);
		// HDFSUtil.deleteNoUse(output);
		// // opt+=F1.job(output, output+"_HIT");//计算F1值
		// // HDFSUtil.deleteNoUse(output+"_HIT");
		// outputPrint.append(opt+"\n");

	

		// // 加上一部分数据，再做一次
		// output = outputFolder + "/F0_PLUS_m22";
		// plus = Aconst.rootPath + "bought/m22.txt";
		// opt=F0.job(inputs, output, plus);
		// HDFSUtil.deleteNoUse(output);
		// outputPrint.append(opt+"\n");

		
		System.out.println("\n本次merge执行结果:\n" + outputPrint.toString());
		return outputPrint.toString() + "\n";

	}

	/**
	 * 均匀分布
	 * 
	 * @param startDay
	 * @param endDay
	 * @param startScore
	 * @throws Exception
	 */
	public static String uniform(int startScore, String startDay, String endDay, int incrScore, int increDay) throws Exception {
		String outputFolder = Aconst.rootPathPlusDatePlusWs + "UNIFORM/";
		List<String[]> sdsList = new ArrayList<String[]>();
		int score = startScore;
		float ed = Float.valueOf(endDay);
		do {
			sdsList.add(new String[] { String.valueOf(score), startDay });

			score += incrScore;
			startDay = MyDate.getDaysBefore(startDay, increDay);
		} while (Float.valueOf(startDay) > ed);

		outputFolder += "ss=" + sdsList.get(0)[0] + "_sd=" + sdsList.get(0)[1] + "_es=" + sdsList.get(sdsList.size() - 1)[0] + "_ed=" + sdsList.get(sdsList.size() - 1)[1];

		return F0.merge(sdsList, outputFolder);

	}

	public static String uniform2(int startScore, String startDay, String endDay, int incrScore, int increDay, String startDay2, String endDay2, int increDay2) throws Exception {
		String outputFolder = Aconst.rootPathPlusDatePlusWs + "UNIFORM2/";
		List<String[]> sdsList = new ArrayList<String[]>();
		int score = startScore;
		do {
			sdsList.add(new String[] { String.valueOf(score), startDay });
			score += incrScore;
			startDay = MyDate.getDaysBefore(startDay, increDay);
		} while (Float.valueOf(startDay) > Float.valueOf(endDay));

		do {
			sdsList.add(new String[] { String.valueOf(score), startDay2 });
			score += incrScore;
			startDay2 = MyDate.getDaysBefore(startDay2, increDay2);
		} while (Float.valueOf(startDay2) > Float.valueOf(endDay2));

		outputFolder += "ss=" + sdsList.get(0)[0] + "_sd=" + sdsList.get(0)[1] + "_es=" + sdsList.get(sdsList.size() - 1)[0] + "_ed=" + sdsList.get(sdsList.size() - 1)[1];

		return F0.merge(sdsList, outputFolder);

	}

	/**
	 * 斐波纳契
	 * 
	 * @param startDay
	 * @param endDay
	 * @param startScore
	 * @throws Exception
	 */
	public static void fibonacci(String startDay, String endDay) throws Exception {

		String outputFolder = Aconst.rootPath + Aconst.date + "FIBONACCI/";
		outputFolder += "sd=" + startDay + "_ed=" + endDay;

		List<String[]> sdsList = new ArrayList<String[]>();

		int s1 = 1;
		int s2 = 2;
		int sTemp;

		int d1 = 3;
		int d2 = 5;
		int dTemp;
		float ed = Float.valueOf(endDay);
		do {
			sTemp = s1 + s2;
			s1 = s2;
			s2 = sTemp;

			startDay = MyDate.getDaysBefore(startDay, d2);

			dTemp = d1 + d2;
			d1 = d2;
			d2 = dTemp;

			sdsList.add(new String[] { String.valueOf(s2), startDay });

		} while (Float.valueOf(startDay) > ed);

		System.out.println("max_score=" + sTemp);
		System.out.println("min_day=" + startDay);

		outputFolder += "ss=" + sdsList.get(0)[0] + "_sd=" + sdsList.get(0)[1] + "_es=" + sdsList.get(sdsList.size() - 1)[0] + "_ed=" + sdsList.get(sdsList.size() - 1)[1];

		F0.merge(sdsList, outputFolder);
	}

	/**
	 * 测试多个
	 * 
	 * @throws Exception
	 */
	public static void TestMutilUniform() throws Exception {
		StringBuffer sb = new StringBuffer();
		// 最开始分数
		int startScore = 3;
		// 分数加1
		int incrScore = 1;
		// 以3天为跨度相加;
		int increDay = 3;
		String firstDay = String.valueOf(Aconst.firstDay);
		String startDay = MyDate.getDaysBefore(firstDay, 5);// 8.10
		String endDay = MyDate.getDaysBefore(firstDay, 15);
		// String endDay ="7.20";
		do {
			String opt = uniform(startScore, startDay, endDay, incrScore, increDay);
			sb.append(opt);
			endDay = MyDate.getDaysBefore(endDay, 3);
			// } while (Float.valueOf(endDay) > (Aconst.lastDay));
		} while (Float.valueOf(endDay) > ((float) 7.20));

		System.out.println("多组测试的结果总汇:\n" + sb.toString());
	}

	public static void TestUniform(String endDay) throws Exception {
		// 最开始分数
		int startScore = 3;
		// 分数加1
		int incrScore = 1;
		// 以3天为跨度相加;
		int increDay = 3;
		String firstDay = String.valueOf(Aconst.firstDay);
		String startDay = MyDate.getDaysBefore(firstDay, 5);// 8.10

		uniform(startScore, startDay, endDay, incrScore, increDay);
	}

	public static void TestUniform2() throws Exception {
		// 最开始分数
		int startScore = 3;
		// 分数加1
		int incrScore = 1;
		// 以2天为跨度相加;
		int increDay = 3;
		String firstDay = String.valueOf(Aconst.firstDay);
		String startDay =  MyDate.getDaysBefore(firstDay, 5);// 8.10
		String endDay=MyDate.getDaysBefore(firstDay, 31);
		
	
		
		String startDay2 =  endDay;// 8.10
		String endDay2="6.11";
		// 以2天为跨度相加;
		int increDay2 = 2;

		uniform2(startScore, startDay, endDay, incrScore, increDay,startDay2, endDay2, increDay2);
	}

	public static void TestFibonacci() throws Exception {
		fibonacci("8.15", "6.01");
	}

	public static void main(String args[]) throws Exception {

		// 1 ,1加上

		String sd1[][] = { { "3", "8.10" }, { "5", "8.01" }, { "7", "7.15" }, { "9", "6.25" }, };

		String sd2[][] = { { "3", "8.10" }, { "5", "8.01" }, { "7", "7.15" }, { "9", "6.25" }, { "11", "6.01" } };

		String sd3[][] = { { "3", "8.10" }, { "5", "8.01" }, { "8", "7.15" }, { "13", "6.25" }, { "21", "6.01" } };

		String sd4[][] = { { "3", "8.10" }, { "5", "8.01" }, { "8", "7.15" }, { "13", "6.25" }, { "21", "6.01" }, { "33", "5.01" } };

		String sd5[][] = { { "3", "8.10" }, { "6", "8.01" }, { "9", "7.15" }, { "12", "6.25" }, };

		String sd6[][] = { { "3", "8.10" }, { "5", "8.01" }, { "8", "7.15" }, { "13", "6.25" }, };

		// F0.merge(sd1);
		// F0.merge(sd2);
		// F0.merge(sd3);
		// F0.merge(sd4);
		// F0.merge(sd5);

		// TestFibonacci();

		// TestMutilUniform();

		// F0.merge(sd6);

		// TestUniform("6.25");

		// TestUniform("6.28");

		// TestUniform("6.22");

//		TestUniform2();
		TestUniform("6.11");
	}
}
