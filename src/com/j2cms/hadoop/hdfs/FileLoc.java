package com.j2cms.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileLoc {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration ();
		FileSystem hdfs = FileSystem.get(conf);
		Path fpath = new Path ("/user/hadoop/input/file1.txt");
		FileStatus fileStatus = hdfs.getFileStatus(fpath);
		
		BlockLocation[] blockLocations = hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		int blockLen = blockLocations.length;
		for(int i=0;i<blockLocations.length;i++){
			String [] hosts = blockLocations[i].getHosts();
			System.out.println("block_"+i+"_location:");
			for(int j =0;j<hosts.length;j++){
				System.out.println(hosts[j]+"  ");
			}
		}

	}

}
