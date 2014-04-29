package org.robby.mr.shortestpath3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import redis.clients.jedis.Jedis;

public class FindShortestMap  extends Mapper<Text, Text, Text, Text>{

	Jedis jedis;
	List<String> allNodes;
	
	protected void setup(Context context){
		jedis = new Jedis("218.193.154.135");
		//读入所有结点的数组
		allNodes = jedis.lrange("all_nodes", 0, -1);
	}
	

	public static Node getNodeById(String id,HashMap<String,Node> L){
		return L.get(id);
	}
	
	//这种遍历的方式存在效率问题
	public static Node getNodeByName(String n, List<Node> L){
		for(Node node:L){
			if(n.equals(node.getId())){
				return node;
			}
		}
		return null;
	}
	
	
	// Sort  limit 1
	
	
	//效率有问题
	public static Node getShortestNode(List<Node> L){
		Node n = null;
		int d = Integer.MAX_VALUE;
		for(int i=0; i<L.size(); i++)
		{
			Node node = L.get(i);
			if(node.getDistance() <= d){
				n = node;
				d = n.getDistance();
			}
		}
		
		//从没有遍历过的顶点集合，删除已经找到的这个结点
		for(int i=0; i<L.size(); i++)
		{
			Node node = L.get(i);
			if(node.getId().equals(n.getId())){
				L.remove(node);
			}
		}
		return n;
	}
	
	//效率有问题
	public static boolean exist(String name, List<Node> L){
		for(Node node:L){
			if(name.equals(node.getId())){
				return true;
			}
		}
		return false;
	}
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		
		//表示所有没有遍历过的顶点集合
		Map<String,Node> TT = new HashMap<String,Node>(); 
		
		//表示所有没有遍历过的顶点集合
		List<Node> T = new ArrayList<Node>();//我觉得问题在于数据量太大内存是放不下的。如果内存足够的话，又何必用hadoop呢
		
		//表示已经找到最短路径的顶点集合
		List<Node> S = new ArrayList<Node>();

		//效率有问题。每次map都要初始化一次
		for(String name: allNodes){
			T.add(new Node(name, Integer.MAX_VALUE));
		}
		
		//key为startNode,自己距离自己的距离是0
		getNodeByName(key.toString(), T).setDistance(0);
		
		while(T.size() > 0){
			Node n = getShortestNode(T);//在第一次迭代时找到的距离key最短的节点是key自己
			System.out.println("1:" + n.getId());
			
			
			//从数据库获取当前结点的相邻结点
			List<String> adjNodes = jedis.lrange("node_" + n.getId(), 0, -1);
			
			
			int distance = n.getDistance() + 1;
			String bp;
			if(n.getBackpointer() == null)
				bp = n.getId();
			else
				bp = n.getBackpointer() + ":" + n.getId();
			for(String adjName:adjNodes){
				if(!exist(adjName, S)){
					Node adjNode = getNodeByName(adjName, T);
					if(adjNode.getDistance() > distance){
						adjNode.setDistance(distance);
						adjNode.setBackpointer(bp);
					}
				}
			}
			S.add(n);
		}
		
		for(Node n:S){
			Text t = new Text(key.toString() + "-" + n.getId() + " (" + n.getBackpointer() + ")");
			context.write(t, new Text(""));
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		jedis.disconnect();
	}
	
}
