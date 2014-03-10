package com.j2cms.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CreateDir {

	/**
	 * 创建HDFS目录
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs  = FileSystem.get(conf);
		
		Path dfs = new Path("/TestDir");
		hdfs.mkdirs(dfs);
	}

}
