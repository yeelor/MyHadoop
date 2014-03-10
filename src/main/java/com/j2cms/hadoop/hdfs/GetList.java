package com.j2cms.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class GetList {

	/**获取HDFS集群上的所有节点的名称信息
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		DistributedFileSystem hdfs = (DistributedFileSystem)fs;
		
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		for(int i=0;i<dataNodeStats.length;i++){
			System.out.println("DataNode_"+i+"_Name:"+dataNodeStats[i].getHostName());
		}

	}

}
