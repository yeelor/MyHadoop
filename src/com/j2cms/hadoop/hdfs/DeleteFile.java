package com.j2cms.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DeleteFile {

	/**
	 * 删除HDFS上的文件
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);

		Path deletePath = new Path("/test1");

		boolean isDeleted = hdfs.delete(deletePath, false);// 是否递归删除

		// boolean isDeleted = hdfs.delete(deletePath, true);//递归删除
		System.out.println(isDeleted);
	}

}
