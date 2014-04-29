package cn.edu.ruc.cloudcomputing.book.chapter15;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;

public class JoinGroup extends ZooKeeperInstance {

	
	//加入组操作
	public int join(String groupPath,int k ) throws Exception{
		String child = "child_"+k;
		
		//创建的路径
		String path = groupPath+"/"+child;
		//检查组是否存在
		if(zk.exists(groupPath, true)!=null){
			zk.create(path, (child+"_child").getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			return 1;
		}else{
			System.out.println("组不存在，创建组...");
			zk.create(groupPath, "ZKGroup".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			return 1;
		}
	}
	
	public void multiJoin() throws Exception{
		for(int i =0;i<10;i++){
			int k = this.join("/ZKGroup", i);
			if(k==0)
				break;
		}
	}
	
	public static void main(String args[]) throws Exception{
		JoinGroup jg = new JoinGroup();
		jg.createZKInstance();
		jg.multiJoin();
		jg.closeZK();
	}
}
