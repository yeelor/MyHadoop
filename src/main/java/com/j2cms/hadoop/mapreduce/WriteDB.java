package com.j2cms.hadoop.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class WriteDB {

	public static class Map extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();

			// 处理记事本UTF-8的BOM问题
			if (line.getBytes().length > 0) {
				if ((int) line.charAt(0) == 65279) {
					line = line.substring(1);
				}
			}

			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}

		}
	}

	public static class Combine extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, WordRecord, Text> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<WordRecord, Text> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			WordRecord wordRecord = new WordRecord();
			wordRecord.word = key.toString();
			wordRecord.number = sum;

			output.collect(wordRecord, new Text());
		}
	}

	public static class WordRecord implements Writable, DBWritable {
		public String word;
		public int number;

		@Override
		public void write(PreparedStatement statement) throws SQLException {
			statement.setString(1, this.word);
			statement.setInt(2, this.number);

		}

		@Override
		public void readFields(ResultSet resultSet) throws SQLException {
			this.word = resultSet.getString(1);
			this.number = resultSet.getInt(2);

		}

		@Override
		public void write(DataOutput out) throws IOException {
			Text.writeString(out, this.word);
			out.writeInt(this.number);

		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.word = Text.readString(in);
			this.number = in.readInt();

		}

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(WriteDB.class);
		conf.set("mapred.job.tracker", "192.168.162.128:9001");
		DistributedCache.addFileToClassPath(new Path("/lib/mysql-connector-java-5.1.26-bin.jar"), conf);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(DBOutputFormat.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Combine.class);
		conf.setReducerClass(Reduce.class);

		FileInputFormat.setInputPaths(conf, new Path("input"));

		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://master:3306/school?characterEncoding=UTF-8", "school", "guotao");

		// 写入表中的数据
		String[] fields = { "word", "number" };
		DBOutputFormat.setOutput(conf, "wordcount", fields);

		JobClient.runJob(conf);
	}

}
