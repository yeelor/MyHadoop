package com.j2cms.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CreateFile {

	/**
	 * 创建HDFS文件
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		byte[] buff = "hello hadoop world\n".getBytes();
		Path dfs = new Path("/test");
		FSDataOutputStream outputStream = hdfs.create(dfs);
		outputStream.write(buff, 0, buff.length);
	}

}
