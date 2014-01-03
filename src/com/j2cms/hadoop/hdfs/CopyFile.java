package com.j2cms.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyFile {

	/**
	 * 上传本地文件
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		//本地文件
		//Path src = new Path("/home/hadoop/file/file1.txt");
		
		//本地文件夹
		Path src = new Path("/home/hadoop/file");
				
		//HDFS 位置
		Path  dst = new Path("/");
		
		hdfs.copyFromLocalFile(src, dst);
		
		System.out.println("Upload to "+conf.get("fs.default.name"));
		
		FileStatus files[] = hdfs.listStatus(dst);
		for (FileStatus file:files){
			System.out.println(file.getPath());
		}
		
		

	}

}
