//package com.j2cms.hadoop.mapreduce.canopy;
//
//import java.util.List;
//
//public class MyCanopy {
//
//	// Canopy 聚类算法的内存实现
//	public static void canopyClusterInMemory() {
//		// 设置距离阈值 T1,T2
//		double T1 = 4.0;
//		double T2 = 3.0;
//		// 调用 CanopyClusterer.createCanopies 方法创建 Canopy，参数分别是：
//		// 1. 需要聚类的点集
//		// 2. 距离计算方法
//		// 3. 距离阈值 T1 和 T2
//		List<Canopy> canopies = CanopyClusterer.createCanopies(SimpleDataSet.getPointVectors(SimpleDataSet.points), new EuclideanDistanceMeasure(), T1, T2);
//		// 打印创建的 Canopy，因为聚类问题很简单，所以这里没有进行下一步精确的聚类。
//		// 有必须的时候，可以拿到 Canopy 聚类的结果作为 K 均值聚类的输入，能更精确更高效的解决聚类问题
//		for (Canopy canopy : canopies) {
//			System.out.println("Cluster id: " + canopy.getId() + " center: " + canopy.getCenter().asFormatString());
//			System.out.println("       Points: " + canopy.getNumPoints());
//		}
//	}
//
//	// Canopy 聚类算法的 Hadoop 实现
//	public static void canopyClusterUsingMapReduce() throws Exception {
//		// 设置距离阈值 T1,T2
//		double T1 = 4.0;
//		double T2 = 3.0;
//		// 声明距离计算的方法
//		DistanceMeasure measure = new EuclideanDistanceMeasure();
//		// 设置输入输出的文件路径
//		Path testpoints = new Path("testpoints");
//		Path output = new Path("output");
//		// 清空输入输出路径下的数据
//		HadoopUtil.overwriteOutput(testpoints);
//		HadoopUtil.overwriteOutput(output);
//		// 将测试点集写入输入目录下
//		SimpleDataSet.writePointsToFile(testpoints);
//
//		// 调用 CanopyDriver.buildClusters 的方法执行 Canopy 聚类，参数是：
//		// 1. 输入路径，输出路径
//		// 2. 计算距离的方法
//		// 3. 距离阈值 T1 和 T2
//		new CanopyDriver().buildClusters(testpoints, output, measure, T1, T2, true);
//		// 打印 Canopy 聚类的结果
//		List<List<Cluster>> clustersM = DisplayClustering.loadClusters(output);
//		List<Cluster> clusters = clustersM.get(clustersM.size() - 1);
//		if (clusters != null) {
//			for (Cluster canopy : clusters) {
//				System.out.println("Cluster id: " + canopy.getId() + " center: " + canopy.getCenter().asFormatString());
//				System.out.println("       Points: " + canopy.getNumPoints());
//			}
//		}
//	}
//}
