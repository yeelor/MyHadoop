package com.j2cms.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ListAllFile {

	/**
	 * 读取HDFS某个目录下的所有文件
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		Path  listf = new Path("/user/hadoop/input");
		FileStatus status[] = hdfs.listStatus(listf);
		for(int i =0;i<status.length;i++){
			System.out.println(status[i].getPath().toString());
		}
		hdfs.close();

	}

}
