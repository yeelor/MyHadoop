package com.j2cms.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GetLTime {

	/**
	 * 查看HDFS文件的最后修改时间
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration ();
		FileSystem hdfs = FileSystem.get(conf);
		Path fpath = new Path ("/user/hadoop/input/file1.txt");
		FileStatus fileStatus = hdfs.getFileStatus(fpath);
		long modiTime = fileStatus.getModificationTime();
		System.out.println(modiTime);

	}

}
