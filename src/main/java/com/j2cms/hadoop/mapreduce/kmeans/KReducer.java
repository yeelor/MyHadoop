package com.j2cms.hadoop.mapreduce.kmeans;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KReducer extends Reducer<Text, Text, Text, Text> {
	// <中心点类别,中心点对应的坐标集合>,每个中心点类别的坐标集合求新的中心点
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		System.out.println(key.toString() + "Reduce");
		int dimension = key.toString().replace("(", "").replace(")", "").replace(":", "").split(",").length;
		float[] averages = new float[dimension];

		for (int i = 0; i < dimension; i++)
			averages[i] = 0;
		int count = 0;
		String clusters = "";
		for (Text value : values) {
			System.out.println("value:" + value.toString());
			clusters += value.toString() + " ";
			String[] valueSplits = value.toString().replace("(", "").replace(")", "").split(",");
			System.out.println("计算点的维度:" + valueSplits.length);
			for (int i = 0; i < valueSplits.length; i++)
				averages[i] += Float.parseFloat(valueSplits[i]);
			count++;
		}
		System.out.println("count:" + count);
		System.out.println("clusters:" + clusters);
		for (int i = 0; i < dimension; i++) {
			System.out.println("averages[" + i + "]:" + averages[i]);
		}
		// averages[0]存储X坐标之和，averages[1]存储Y坐标之和
		String center = "";
		for (int i = 0; i < dimension; i++) {
			averages[i] = averages[i] / count;
			if (i == 0)
				center += "(" + averages[i] + ",";
			else if (i == (dimension - 1))
				center += averages[i] + ")";
			else {
				center += averages[i] + ",";
			}
		}
		System.out.println("写入格式:旧聚类中心 包含的聚类点 新聚类中心:" + key + " " + clusters + " " + center);
		context.write(key, new Text(clusters + center));
	}

}
