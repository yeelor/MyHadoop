package com.j2cms.hadoop.hdfs;

public class Test {

	public static void  main(String [] args){
		Integer [] numbers = new Integer[10];
		
		for(int i=0;i<10;i++){
			numbers[i] = (int) (100*Math.random());
			System.out.println(numbers[i]);
		}
		
		int num[][] = new int [2][10];
		
		for(int i=0;i<10;i++){
			num[0][i]=i;
			num[1][i]=(int) (100*Math.random());
			System.out.println(num[0][i]+":"+num[1][i]);
		}
	}
}
