package cn.edu.ruc.cloudcomputing.book.chapter15;

import java.util.List;

public class DelGroup extends ZooKeeperInstance {

	public void delete(String groupPath) throws Exception{
		List<String> children = zk.getChildren(groupPath, false);
		//如果不空，则进行删除
		if(!children.isEmpty()){
			for(String child:children){
				System.out.println("child="+child);
				zk.delete(groupPath+"/"+child, -1);
			}
		}
		//删除组目录节点
		zk.delete(groupPath, -1);
	}
	
	public static void main(String args[]) throws Exception{
		DelGroup  dg = new DelGroup();
		dg.createZKInstance();
		dg.delete("/c");
		dg.closeZK();
	}
}
