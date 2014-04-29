package com.j2cms.hadoop.mapreduce.alibaba;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSUtil {

	/**
	 * 删除两个无关目录
	 * 
	 * @param p
	 * @throws Exception
	 */
	public static void deleteNoUse(String p) throws Exception {
		// conf1.set("mapred.job.tracker", "lenovo0:9001");
		FileSystem hdfs = FileSystem.get(new Configuration());
		Path deletePath = new Path(p + "/_logs");

//		System.out.println("deleteNoUse=" + deletePath);

		hdfs.delete(deletePath, true);// 是否递归删除
		deletePath = new Path(p + "/_SUCCESS");
		boolean isDeleted = hdfs.delete(deletePath, true);
//		System.out.println(isDeleted);
	}

	/**
	 * 删除目录
	 * 
	 * @param path
	 * @throws Exception
	 */
	public static void deletePath(String path) throws Exception {

		FileSystem hdfs = FileSystem.get(new Configuration());
		Path deletePath = new Path(path);

		System.out.println("deletePath=" + deletePath);

		boolean isDeleted = hdfs.delete(deletePath, true);// 是否递归删除

		System.out.println(isDeleted);
	}

	/**
	 * 移动目录里文件到userIdbBrand目录,方便统一处理
	 * 
	 * @param ouput
	 * @throws Exception
	 */
	public static void mv(String src, String dst) throws Exception {
		System.out.println("mv_src=" + src);
		System.out.println("mv_dst=" + dst);
		FileSystem hdfs = FileSystem.get(new Configuration());
		hdfs.rename(new Path(src), new Path(dst));

	}

	/**
	 * 移动目录里文件到userIdbBrand目录,方便统一处理
	 * 
	 * @param ouput
	 * @throws Exception
	 */
	public static void copyFromLocalFile(String src, String dst) throws Exception {
		System.out.println("copy_src=" + src);
		System.out.println("copy_dst=" + dst);
		FileSystem hdfs = FileSystem.get(new Configuration());
		hdfs.copyFromLocalFile(new Path(src), new Path(dst));
	}

	/**
	 * 建目录
	 * 
	 * @param ouput
	 * @throws Exception
	 */
	public static void mkdirs(String path) throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		hdfs.mkdirs(new Path(path));
	}
	
	/**
	 * 检查是否存在某个目录或者文件 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static boolean exists(String path) throws Exception {
		FileSystem hdfs = FileSystem.get(new Configuration());
		return hdfs.exists(new Path(path));
	}

	/**
	 * 写入文件
	 * @param path
	 * @param s
	 * @throws Exception
	 */
	public static void write(String path,String s ) throws Exception{
		FileSystem fs = FileSystem.get(new Configuration());
		OutputStream out = fs.create(new Path(path) ); 
		IOUtils.copyBytes(new ByteArrayInputStream(s.getBytes()), out, 4096,true);
		out.close();
	}
}
