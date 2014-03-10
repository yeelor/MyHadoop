package org.robby.mr.shortestpath2;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * 不必写reduce
 * @author hadoop
 *
 */
public class Reduce extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		context.write(key, new Text(""));
	}
}
