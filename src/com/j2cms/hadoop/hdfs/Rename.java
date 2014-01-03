package com.j2cms.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Rename {

	/**
	 * 重命名HDFS文件
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		Path src = new Path("/test"); //旧的文件名
		Path dst = new Path ("/test1"); //新的文件名
			
		
		boolean isRename = hdfs.rename(src, dst);
		
		System.out.println(isRename?"成功":"失败");

	}

}
