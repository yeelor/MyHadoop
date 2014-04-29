package cn.edu.ruc.cloudcomputing.book.chapter15;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

public class CreateGroup extends ZooKeeperInstance {

	//创建组
	public void createPNode(String groupPath) throws KeeperException, InterruptedException{
		String cGroupPath = zk.create(groupPath, "group".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(cGroupPath);
	}
	
	public static void main(String args []) throws Exception{
		CreateGroup cg = new CreateGroup();
		cg.createZKInstance();
		cg.createPNode("/ZKGroup");
		cg.closeZK();
	}
}
