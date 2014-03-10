package org.robby.mr.count;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import redis.clients.jedis.Jedis;

/**
 * 将输出写入到redis内存数据库
 * @author hadoop
 *
 * @param <K>
 * @param <V>
 */
public class RedisOutputFormat<K,V> extends FileOutputFormat<K, V> {

	protected static class RedisRecordWriter<K,V> extends RecordWriter<K,V>{

		private Jedis jedis;
		
		public RedisRecordWriter(Jedis jedis) {

			this.jedis = jedis;
		}

		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			boolean nullkey = key == null;
			boolean nullvalue = value == null;
			
			if(nullkey || nullvalue){
				return;
			}
			
			String s = key.toString();
			int score = Integer.parseInt(value.toString());
			for(int i=0; i<s.length(); i++){
				String k = s.substring(0, i+1);
				System.out.println(k);
				jedis.zincrby(k, score, s);
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			jedis.disconnect();
		}
		
		
		
	}
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		
		return new RedisRecordWriter<K,V>(new Jedis("127.0.00.1"));
	}

}
