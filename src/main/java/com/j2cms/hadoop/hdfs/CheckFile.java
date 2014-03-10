package com.j2cms.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



public class CheckFile {

	/**
	 * 查看HDFS文件是否存在
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path findPath = new Path("/file1.txt");
		boolean isExists = hdfs.exists(findPath);
		System.out.println(isExists);

	}

}
