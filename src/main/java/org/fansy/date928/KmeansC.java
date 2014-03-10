package org.fansy.date928;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansC extends Reducer<IntWritable,DataPro,IntWritable,DataPro> {
	private static int dimension=0;
	
	private static Log log =LogFactory.getLog(KmeansC.class);
	// the main purpose of the sutup() function is to get the dimension of the original data
	public void setup(Context context) throws IOException{
		Path[] caches=DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if(caches==null||caches.length<=0){
			log.error("center file does not exist");
			System.exit(1);
		}
		BufferedReader br=new BufferedReader(new FileReader(caches[0].toString()));
		String line;
		while((line=br.readLine())!=null){
			String[] str=line.split("\t");
		//	String[] str=line.split("\\s+");
			dimension=str.length;
			break;
		}
		try {
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public void reduce(IntWritable key,Iterable<DataPro> values,Context context)throws InterruptedException, IOException{	
		double[] sum=new double[dimension];
		int sumCount=0;
		// operation two
		for(DataPro val:values){
			String[] datastr=val.getCenter().toString().split("\t");
	//		String[] datastr=val.getCenter().toString().split("\\s+");
			sumCount+=val.getCount().get();
			for(int i=0;i<dimension;i++){
				sum[i]+=Double.parseDouble(datastr[i]);
			}
		}
		//  calculate the new centers
//		double[] newcenter=new double[dimension];
		StringBuffer sb=new StringBuffer();
		for(int i=0;i<dimension;i++){
			sb.append(sum[i]+"\t");
		}
	//	System.out.println("combine text:"+sb.toString());
	//	System.out.println("combine sumCount:"+sumCount);
		DataPro newvalue=new DataPro();
		newvalue.set(new Text(sb.toString()), new IntWritable(sumCount));
		context.write(key, newvalue);
	}
}
