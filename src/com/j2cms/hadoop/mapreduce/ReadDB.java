package com.j2cms.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class ReadDB {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, StudentRecord, LongWritable, Text> {

		// 实现map函数
		public void map(LongWritable key, StudentRecord value, OutputCollector<LongWritable, Text> collector, Reporter reporter) throws IOException {
			collector.collect(new LongWritable(value.id), new Text(value.toString()));
		}

	}

	public static class StudentRecord implements Writable, DBWritable {
		public int id;
		public String name;
		public String sex;
		public int age;

		@Override
		public void readFields(DataInput in) throws IOException {
			this.id = in.readInt();
			this.name = Text.readString(in);
			this.sex = Text.readString(in);
			this.age = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(this.id);
			Text.writeString(out, this.name);
			Text.writeString(out, this.sex);
			out.writeInt(this.age);
		}

		@Override
		public void readFields(ResultSet result) throws SQLException {
			this.id = result.getInt(1);
			this.name = result.getString(2);
			this.sex = result.getString(3);
			this.age = result.getInt(4);
		}

		@Override
		public void write(PreparedStatement stmt) throws SQLException {
			stmt.setInt(1, this.id);
			stmt.setString(2, this.name);
			stmt.setString(3, this.sex);
			stmt.setInt(4, this.age);
		}

		@Override
		public String toString() {
			return new String("学号：" + this.id + "_姓名：" + this.name + "_性别：" + this.sex + "_年龄：" + this.age);
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(ReadDB.class);

		// 这句话很关键
		conf.set("mapred.job.tracker", "192.168.162.128:9001");

		// 非常重要，值得关注
		DistributedCache.addFileToClassPath(new Path("/lib/mysql-connector-java-5.1.26-bin.jar"), conf);

		// 设置输入类型
		conf.setInputFormat(DBInputFormat.class);

		// 设置输出类型
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		// 设置Map和Reduce类
		conf.setMapperClass(Map.class);
		conf.setReducerClass(IdentityReducer.class); //Performs no reduction, writing all input values directly to the output. 

		// 设置输出目录
		FileOutputFormat.setOutputPath(conf, new Path("rdb_out_"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())));

		// 建立数据库连接
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://master:3306/school", "school", "guotao");

		// 读取“student”表中的数据
		String[] fields = { "id", "name", "sex", "age" };
		DBInputFormat.setInput(conf, StudentRecord.class, "student", null, "id", fields);

		JobClient.runJob(conf);
	}
}